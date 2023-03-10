
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('get-characters').getOrCreate()
sc = spark.sparkContext

ts = 1678335199  # int(time.time())
inputs = f"./case/landing/characters/uploaded_at={ts}/characters_0.json"
outputs = f"./case/silver/characters/uploaded_at={ts}/"
os.makedirs(os.path.dirname(outputs), exist_ok=True)

charactersDF = spark.read.json(inputs)
# charactersDF.select(charactersDF.data.results).withColumn("result", explode(results.show()
resultDF = charactersDF.select(F.explode(charactersDF.data.results).alias("result"))
# resultDF.printSchema()
reducedCols = resultDF.select(
    'result.id',
    'result.name',
    'result.description',
    resultDF.result.comics.available.alias('comics_available'),
    resultDF.result.events.available.alias('events_available'),
    resultDF.result.stories.available.alias('stories_available'),
    resultDF.result.series.available.alias('series_available'),
    F.current_date().alias('run_date')
)
# reducedCols.printSchema()
reducedCols.write.parquet(outputs, 'overwrite')
