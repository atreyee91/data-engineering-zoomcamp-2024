```python
import json
import time 

from kafka import KafkaProducer
```


```python
def json_serializer(data):
    return json.dumps(data).encode('utf-8')
```


```python
server = 'localhost:9093'
```


```python
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
```


```python
producer.bootstrap_connected()
```


```python
t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```


```python
import time

t0 = time.time()

topic_name = 'test-topic'

# Time spent in sending messages
send_start_time = time.time()
for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)
send_end_time = time.time()

# Time spent in flushing
flush_start_time = time.time()
producer.flush()
flush_end_time = time.time()

t1 = time.time()

# Calculate total time taken
total_time = t1 - t0

# Calculate time spent in sending messages
send_time = send_end_time - send_start_time

# Calculate time spent in flushing
flush_time = flush_end_time - flush_start_time

print(f'Total time taken: {total_time:.2f} seconds')
print(f'Time spent in sending messages: {send_time:.2f} seconds')
print(f'Time spent in flushing: {flush_time:.2f} seconds')

```


```python
import pyspark
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```


```python
!gzip -dc green_tripdata_2019-10.csv.gz
```


```python
!wc -l green_tripdata_2019-10.csv
```


```python
df_green = spark.read \
    .option("header", "true") \
    .csv('green_tripdata_2019-10.csv')
```


```python
df_green.schema
```


```python
df_green = df_green['lpep_pickup_datetime',
'lpep_dropoff_datetime',
'PULocationID',
'DOLocationID',
'passenger_count',
'trip_distance',
'tip_amount']
```


```python
df_green.show(10)
```


```python
import pandas as pd
```


```python
df_green =pd.read_csv('green_tripdata_2019-10.csv')
```


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
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    print(row_dict)
    break
```


```python
t0 = time.time()

topic_name = 'green-topic'

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

```
