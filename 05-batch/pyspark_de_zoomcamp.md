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
spark.version
```




    '3.5.0'




```python
df = spark.read \
    .option("header", "true") \
    .csv('C:/Users/scl/Downloads/fhv_tripdata_2019-10.csv')
```


```python
df.head(2)
```




    [Row(dispatching_base_num='B00009', pickup_datetime='2019-10-01 00:23:00', dropOff_datetime='2019-10-01 00:35:00', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00009'),
     Row(dispatching_base_num='B00013', pickup_datetime='2019-10-01 00:11:29', dropOff_datetime='2019-10-01 00:13:22', PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00013')]




```python
df.schema
```




    StructType([StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', StringType(), True), StructField('dropOff_datetime', StringType(), True), StructField('PUlocationID', StringType(), True), StructField('DOlocationID', StringType(), True), StructField('SR_Flag', StringType(), True), StructField('Affiliated_base_number', StringType(), True)])




```python
import pandas as pd
```


```python
df_pandas = pd.read_csv('C:/Users/scl/Downloads/fhv_tripdata_2019-10.csv')
```


```python
spark.createDataFrame(df_pandas).schema
```


```python
from pyspark.sql import types
```


```python
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.StringType(), True), 
    types.StructField('DOlocationID', types.StringType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
```


```python
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('C:/Users/scl/Downloads/fhv_tripdata_2019-10.csv')
```


```python
df.head(1)
```




    [Row(dispatching_base_num='B00009', pickup_datetime=datetime.datetime(2019, 10, 1, 0, 23), dropOff_datetime=datetime.datetime(2019, 10, 1, 0, 35), PUlocationID='264', DOlocationID='264', SR_Flag=None, Affiliated_base_number='B00009')]




```python
df = df.repartition(6)
```


```python
df.write.format("parquet").save("test3/fhvhv/2021/01")
```



```python
from pyspark.sql import functions as F
```


```python
df = df.withColumn('pickup_date',F.to_date(df.pickup_datetime)) \
  .withColumn('dropOff_date',F.to_date(df.dropOff_datetime))
```


```python
df.show(3)
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|pickup_date|dropOff_date|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+
    |              B01259|2019-10-01 12:11:01|2019-10-01 12:32:47|         264|          45|   NULL|                B01259| 2019-10-01|  2019-10-01|
    |              B02881|2019-10-02 08:30:24|2019-10-02 09:30:25|          82|         244|   NULL|                B02881| 2019-10-02|  2019-10-02|
    |              B02293|2019-10-01 12:05:29|2019-10-01 12:45:38|          97|          63|   NULL|                B02293| 2019-10-01|  2019-10-01|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+
    only showing top 3 rows
    
    


```python
df.filter(df.pickup_date == '2019-10-15').count()
```




    62610




```python
df.withColumn('duration',F.date_diff(df.pickup_datetime,df.pickup_datetime)).show(3)
```

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+--------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|pickup_date|dropOff_date|duration|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+--------+
    |              B01259|2019-10-01 12:11:01|2019-10-01 12:32:47|         264|          45|   NULL|                B01259| 2019-10-01|  2019-10-01|       0|
    |              B02881|2019-10-02 08:30:24|2019-10-02 09:30:25|          82|         244|   NULL|                B02881| 2019-10-02|  2019-10-02|       0|
    |              B02293|2019-10-01 12:05:29|2019-10-01 12:45:38|          97|          63|   NULL|                B02293| 2019-10-01|  2019-10-01|       0|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+------------+--------+
    only showing top 3 rows
    
    


```python
df = df.withColumn('duration', \
        (F.unix_timestamp(df.dropOff_datetime)-F.unix_timestamp(df.pickup_datetime)).cast('int')/3600)
```


```python
df = df.filter(F.year(df.dropOff_date)<2029)
```


```python
df.select(F.max(df.duration)).show()
```

    +-----------------+
    |    max(duration)|
    +-----------------+
    |70128.02805555555|
    +-----------------+
    
    


```python
df_z = spark.read \
    .option("header", "true") \
    .csv('C:/Users/scl/Downloads/taxi_zone_lookup.csv')
```


```python
df_z.show(3)
```

    +----------+-------+--------------------+------------+
    |LocationID|Borough|                Zone|service_zone|
    +----------+-------+--------------------+------------+
    |         1|    EWR|      Newark Airport|         EWR|
    |         2| Queens|         Jamaica Bay|   Boro Zone|
    |         3|  Bronx|Allerton/Pelham G...|   Boro Zone|
    +----------+-------+--------------------+------------+
    only showing top 3 rows
    
    


```python
df_1 = df.groupBy(df.PUlocationID).count()
```


```python
min_count = df_1.agg({"count": "min"}).collect()[0][0]

```

    +------------+-----+
    |PUlocationID|count|
    +------------+-----+
    |           2|    1|
    +------------+-----+
    
    


```python
df_final = df_1.join(df_z,df_1.PUlocationID == df_z.LocationID, 'inner')
```


```python
df_final.filter(df_final["count"] == min_count).show()
```

    +------------+-----+----------+-------+-----------+------------+
    |PUlocationID|count|LocationID|Borough|       Zone|service_zone|
    +------------+-----+----------+-------+-----------+------------+
    |           2|    1|         2| Queens|Jamaica Bay|   Boro Zone|
    +------------+-----+----------+-------+-----------+------------+
    
    


```python

```
