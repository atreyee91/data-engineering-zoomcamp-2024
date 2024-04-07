```python
import pyspark
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()
```


```python
spark
```


```python
import pandas as pd

df_green =pd.read_csv('green_tripdata_2019-10.csv')
```

    C:\Users\scl\AppData\Local\Temp\ipykernel_35164\296474856.py:3: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
      df_green =pd.read_csv('green_tripdata_2019-10.csv')
    


```python
columns = ['lpep_pickup_datetime',
           'lpep_dropoff_datetime',
           'PULocationID',
           'DOLocationID',
           'passenger_count',
           'trip_distance',
           'tip_amount']

df_green = df_green[columns]
```


```python
import datetime as dt
df_green['timestamp'] = dt.datetime.now()
```


```python
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9093'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```


```python
t0 = time.time()

topic_name = 'green-trip'

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    message = row_dict
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```


```python
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "green-trip") \
    .option("startingOffsets", "earliest") \
    .load()
```


```python
green_stream
```


```python
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        display(first_row[0])


```


```python
query.stop()
```


```python
from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())
```


```python
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "green-trip") \
    .option("startingOffsets", "earliest")\
    .option("checkpointLocation", "checkpoint") \
    .load()

def debug_and_process(batch_df, batch_id):
    batch_df.select(F.col("value").cast('STRING')).show(truncate=False)
    parsed_batch = batch_df.select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*")
    
    print(f"Batch ID: {batch_id}")
    parsed_batch.show(truncate=False)

query = green_stream \
    .writeStream \
    .foreachBatch(debug_and_process) \
    .start()

query.awaitTermination()
```


```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col 
popular_destinations = df_green.groupby([(F.window(col("timestamp"), "5 minutes")), "DOLocationID"]).count()

```


```python
popular_destinations
```


```python
 console_query = popular_destinations.writeStream\
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
```


```python

```
