
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('process-events').getOrCreate()
sc = spark.sparkContext

ts = 1678335199  # int(time.time())

event_inputs = f"./case/landing/events/uploaded_at={ts}/events_*.json"
char_event_inputs = f"./case/landing/event_characters/uploaded_at={ts}/char_events_*.json"
event_outputs = f"./case/silver/events/uploaded_at={ts}/"
os.makedirs(os.path.dirname(event_outputs), exist_ok=True)

eventsDF = spark.read.json(event_inputs)
resultDF = eventsDF.select(F.explode(eventsDF.data.results).alias("result"))

# Get a list of character id and name for event_id from the Event data
orig_event_char_DF = resultDF.select(
    'result.id',
    #TODO: Better way to transform an Array Column?
    F.explode(resultDF.result.characters.items).alias('item')
).select(
    'id',
    F.col('item.name').alias('character_name'),
    F.regexp_extract('item.resourceURI', r'characters/(\d*)', 1).alias('character_id')
).select(
    'id',
    F.struct('character_name', 'character_id').alias('character')
).groupBy('id').agg(F.collect_list('character').alias('characters'))

# Get Event Character data
events_charDF = spark.read.json(char_event_inputs)

# Get a list of character id and name for event_id from the Event Character data
extra_event_char_DF = events_charDF.select(
    F.explode(events_charDF.data.results).alias("result")
).withColumn(
    "filename", F.input_file_name()
).select(
    F.regexp_extract('filename', r'char_events_(\d*)_', 1).alias('event_id'),
    F.col('result.id').alias('character_id'),
    F.col('result.name').alias('character_name')
).select(
    'event_id',
    F.struct('character_name', 'character_id').alias('character')
).groupBy('event_id').agg(F.collect_list('character').alias('characters'))

# Bring the original and extra (event, [character]) DFs together and take the extra if it exists
joined_event_character_DF = orig_event_char_DF.join(
    extra_event_char_DF, 'orig_event_char_DF.id' == 'extra_event_char_DF.event_id', 'left'
).select(
    orig_event_char_DF.id,
    F.coalesce(extra_event_char_DF.characters, orig_event_char_DF.characters).alias('characters')
)

# Cut down the resultDF
reducedColsEventDF = resultDF.select(
    'result.id',
    'result.title',
    'result.description',
    'result.start',
    'result.end'
)

# Add the character list data to the event data
reducedColsEventDF.join(
    joined_event_character_DF, 'id', 'left'
).select(
    'id',
    'title',
    'description',
    'characters',
    'start',
    'end',
    F.current_date().alias('run_date')
).write.parquet(event_outputs, 'overwrite')

