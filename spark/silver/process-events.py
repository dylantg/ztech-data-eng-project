
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# from pyspark.sql.functions import explode, transform

spark = SparkSession.builder.appName('process-events').getOrCreate()
sc = spark.sparkContext

ts = 1678335199  # int(time.time())
# event_inputs = f"./case/landing/events/uploaded_at={ts}/events_0.json"
event_inputs = f"./case/landing/events/uploaded_at={ts}/events_*.json"
char_event_inputs = f"./case/landing/event_characters/uploaded_at={ts}/char_events_*.json"
event_outputs = f"./case/silver/events/uploaded_at={ts}/"
os.makedirs(os.path.dirname(event_outputs), exist_ok=True)

eventsDF = spark.read.json(event_inputs)
resultDF = eventsDF.select(F.explode(eventsDF.data.results).alias("result"))

# Get (event_id, [(character_id, character_name)]) from Event Data
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

# Get (event_id, [(character_id, character_name)]) from Event Data
events_charDF = spark.read.json(char_event_inputs)
extra_event_char_DF = events_charDF.select(F.explode(events_charDF.data.results).alias("result")
).withColumn("filename", F.input_file_name()
).select(
    F.regexp_extract('filename', r'char_events_(\d*)_', 1).alias('event_id'),
    F.col('result.id').alias('character_id'),
    F.col('result.name').alias('character_name')
).select(
    'event_id',
    F.struct('character_name', 'character_id').alias('character')
).groupBy('event_id').agg(F.collect_list('character').alias('characters'))

### Join character lists
# j_check = orig_event_char_DF.join(
#     extra_event_char_DF, orig_event_char_DF.id == extra_event_char_DF.event_id, 'left'
# ).select(
#     orig_event_char_DF.id,
#     F.size(extra_event_char_DF.characters),
#     F.size(orig_event_char_DF.characters),
#     F.size(F.coalesce(extra_event_char_DF.characters, orig_event_char_DF.characters))
# )

jDF = orig_event_char_DF.join(
    extra_event_char_DF, orig_event_char_DF.id == extra_event_char_DF.event_id, 'left'
).select(
    orig_event_char_DF.id,
    F.coalesce(extra_event_char_DF.characters, orig_event_char_DF.characters).alias('characters')
)
###
reducedCols = resultDF.select(
    'result.id',
    'result.title',
    'result.description',
    'result.start',
    'result.end'
)

reducedCols.join(jDF, 'id', 'left').select(
    'id',
    'title',
    'description',
    'characters',
    'start',
    'end',
    F.current_date().alias('run_date')
).write.parquet(event_outputs, 'overwrite')

# reducedCols.join(jDF, 'id', 'left').select(
#     'id',
#     'title',
#     'description',
#     'characters',
#     'start',
#     'end',
# ).take(5)
###
dts = reducedCols.select('id', 'start', 'end', F.datediff('end', 'start').alias('event_days'))