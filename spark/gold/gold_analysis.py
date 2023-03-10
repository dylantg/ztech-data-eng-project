import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('gold-analysis').getOrCreate()
sc = spark.sparkContext

ts = 1678335199  # int(time.time())
characters_input = f"./case/silver/characters/uploaded_at={ts}/*.parquet"
events_input = f"./case/silver/events/uploaded_at={ts}/*.parquet"

gold_output_q1 = f"./case/gold/uploaded_at={ts}/q1/"
gold_output_q2 = f"./case/gold/uploaded_at={ts}/q2/"
gold_output_q3 = f"./case/gold/uploaded_at={ts}/q3/"
os.makedirs(os.path.dirname(gold_output_q1), exist_ok=True)
os.makedirs(os.path.dirname(gold_output_q2), exist_ok=True)
os.makedirs(os.path.dirname(gold_output_q3), exist_ok=True)

# Get character data
charactersDF = spark.read.parquet(characters_input).select(
    'id',
    'name',
    'comics_available',
    'events_available',
    'stories_available',
    'series_available'
).orderBy(F.col('comics_available').desc())

# Get a DF of the characters with the 10 highest comic available count
top10DF = spark.createDataFrame(charactersDF.take(10))

# Get a list of the character_ids from characters with the 10 highest comic available count
top10_character_ids = [str(x.id) for x in top10DF.select('id')]

###############

eventsDF = spark.read.parquet(events_input)

# Get event data with character data exploded
characterEventDF = eventsDF.select(
    F.explode(eventsDF.characters).alias('character'),
    F.col('id').alias('event_id'),
    'start',
    'end',
    F.datediff('end', 'start').alias('event_days')
)

# Get the event data of the top 10 characters
top10_characterEventDF = characterEventDF.where(
    #TODO: Best filtering option?
    F.col('character.character_id').isin(top10_character_ids)
).select(
    'character.character_id',
    'character.character_name',
    'event_id',
    'start',
    'end',
    F.sequence(F.year('start'), F.year('end')).alias('years'),
    'event_days'
)

# For each character, returns distinct years they had events, the sum of the event days, and set of event_ids
top10_charEvAggDF = top10_characterEventDF.groupby('character_id', 'character_name').agg(
    F.array_distinct(F.flatten(F.collect_list('years'))).alias('event_years')
    , F.sum('event_days').alias('total_event_days')
    , F.collect_set('event_id').alias('event_ids')
)

# Join the character data and character event data together
top10_joined = top10DF.join(top10_charEvAggDF, F.col('id') == F.col('character_id'), 'left')

# Q1: Counts for the top 10 Character
q1 = top10_joined.select(
    'character_name',
    'character_id',
    'comics_available',
    'events_available',
    'stories_available',
    'series_available',
    'total_event_days'
)
q1.write.parquet(gold_output_q1, 'overwrite')

# Q2: (Character, Year): Event count
q2 = characterEventDF.select(
    'character.character_id',
    F.explode(F.sequence(F.year('start'), F.year('end'))).alias('event_year')
).groupby(
    'event_year'
).agg(
    F.size(F.collect_set('character_id')).alias('distinct_characters')
).orderBy(F.col('event_year'))
q2.write.parquet(gold_output_q2, 'overwrite')


# Q3: The count of distinct characters who have an event in a year
q3 = top10_characterEventDF.select(
    F.explode('years').alias('event_year'),
    'character_name',
    'event_id'
).groupby(
    'character_name',
    'event_year'
).agg(
    F.size(F.collect_set('event_id')).alias('distinct_events')
).orderBy(
    F.col('character_name'),
    F.col('event_year')
)
q3.write.parquet(gold_output_q3, 'overwrite')
