import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('gold-analysis').getOrCreate()
sc = spark.sparkContext

ts = 1678335199  # int(time.time())
characters_input = f"./case/silver/characters/uploaded_at={ts}/*.parquet"
events_input = f"./case/silver/events/uploaded_at={ts}/*.parquet"

gold_output_q1 = f"./case/gold/uploaded_at={ts}/q1/"
os.makedirs(os.path.dirname(gold_output_q1), exist_ok=True)
gold_output_q2 = f"./case/gold/uploaded_at={ts}/q2/"
os.makedirs(os.path.dirname(gold_output_q2), exist_ok=True)
gold_output_q3 = f"./case/gold/uploaded_at={ts}/q3/"
os.makedirs(os.path.dirname(gold_output_q3), exist_ok=True)

charactersDF = spark.read.parquet(characters_input)
cDF = charactersDF.select(
    'id',
    'name',
    'comics_available',
    'events_available',
    'stories_available',
    'series_available'
).orderBy(charactersDF.comics_available.desc())
top10DF = spark.createDataFrame(cDF.orderBy(charactersDF.comics_available.desc()).take(10))
top10_character_list = cDF.orderBy(charactersDF.comics_available.desc()).select('id').take(10)
top10_ids = [str(x.id) for x in top10_character_list]

###############
eventsDF = spark.read.parquet(events_input)
charEvDF = eventsDF.select(
    F.explode(eventsDF.characters).alias('character'),
    F.col('id').alias('event_id'),
    'start',
    'end',
    F.datediff('end', 'start').alias('event_days')
)
top10_charEvDF = charEvDF.where(F.col('character.character_id').isin(top10_ids)).select(
    'character.character_id',
    'character.character_name',
    'event_id',
    'start',
    'end',
    # F.sequence(F.year(F.date_trunc('Year', 'start')), F.year(F.date_trunc('Year', 'end'))).alias('years'),
    F.sequence(F.year('start'), F.year('end')).alias('years'),
    'event_days'
)

top10_charEvAggDF = top10_charEvDF.groupby('character_id', 'character_name').agg(
    F.array_distinct(F.flatten(F.collect_list('years'))).alias('event_years')
    , F.sum('event_days').alias('total_event_days')
    , F.collect_set('event_id').alias('event_ids')
)

###
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
q2 = charEvDF.select(
    'character.character_id',
    F.explode(F.sequence(F.year('start'), F.year('end'))).alias('event_year')
).groupby(
    'event_year'
).agg(
    F.size(F.collect_set('character_id')).alias('distinct_characters')
).orderBy(F.col('event_year'))
q2.write.parquet(gold_output_q2, 'overwrite')

# q2a = top10_charEvDF.select(
#     'character_id',
#     'character_name',
#     'event_id',
#     F.explode('years').alias('event_year')
# ).groupby(
#     'character_id', 'character_name', 'event_year'
# ).agg(
#     F.size(F.collect_set('event_id')).alias('distinct_events')
# ).orderBy(F.col('character_name'), F.col('event_year'))

# Q3: By year Dist count of chars
q3 = top10_charEvDF.select(
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



