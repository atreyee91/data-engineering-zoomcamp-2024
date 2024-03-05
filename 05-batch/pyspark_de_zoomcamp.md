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


    ---------------------------------------------------------------------------

    Py4JJavaError                             Traceback (most recent call last)

    Cell In[18], line 1
    ----> 1 df.write.format("parquet").save("test3/fhvhv/2021/01").show()
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pyspark\sql\readwriter.py:1463, in DataFrameWriter.save(self, path, format, mode, partitionBy, **options)
       1461     self._jwrite.save()
       1462 else:
    -> 1463     self._jwrite.save(path)
    

    File C:\ProgramData\anaconda3\Lib\site-packages\py4j\java_gateway.py:1322, in JavaMember.__call__(self, *args)
       1316 command = proto.CALL_COMMAND_NAME +\
       1317     self.command_header +\
       1318     args_command +\
       1319     proto.END_COMMAND_PART
       1321 answer = self.gateway_client.send_command(command)
    -> 1322 return_value = get_return_value(
       1323     answer, self.gateway_client, self.target_id, self.name)
       1325 for temp_arg in temp_args:
       1326     if hasattr(temp_arg, "_detach"):
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pyspark\errors\exceptions\captured.py:179, in capture_sql_exception.<locals>.deco(*a, **kw)
        177 def deco(*a: Any, **kw: Any) -> Any:
        178     try:
    --> 179         return f(*a, **kw)
        180     except Py4JJavaError as e:
        181         converted = convert_exception(e.java_exception)
    

    File C:\ProgramData\anaconda3\Lib\site-packages\py4j\protocol.py:326, in get_return_value(answer, gateway_client, target_id, name)
        324 value = OUTPUT_CONVERTER[type](answer[2:], gateway_client)
        325 if answer[1] == REFERENCE_TYPE:
    --> 326     raise Py4JJavaError(
        327         "An error occurred while calling {0}{1}{2}.\n".
        328         format(target_id, ".", name), value)
        329 else:
        330     raise Py4JError(
        331         "An error occurred while calling {0}{1}{2}. Trace:\n{3}\n".
        332         format(target_id, ".", name, value))
    

    Py4JJavaError: An error occurred while calling o127.save.
    : java.lang.RuntimeException: java.io.FileNotFoundException: Hadoop bin directory does not exist: C:\Users\scl\tools\hadoop-3.2.0\bin -see https://wiki.apache.org/hadoop/WindowsProblems
    	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:735)
    	at org.apache.hadoop.util.Shell.getSetPermissionCommand(Shell.java:270)
    	at org.apache.hadoop.util.Shell.getSetPermissionCommand(Shell.java:286)
    	at org.apache.hadoop.fs.RawLocalFileSystem.setPermission(RawLocalFileSystem.java:978)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkOneDirWithMode(RawLocalFileSystem.java:660)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirsWithOptionalPermission(RawLocalFileSystem.java:700)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirs(RawLocalFileSystem.java:672)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirsWithOptionalPermission(RawLocalFileSystem.java:699)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirs(RawLocalFileSystem.java:672)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirsWithOptionalPermission(RawLocalFileSystem.java:699)
    	at org.apache.hadoop.fs.RawLocalFileSystem.mkdirs(RawLocalFileSystem.java:672)
    	at org.apache.hadoop.fs.ChecksumFileSystem.mkdirs(ChecksumFileSystem.java:788)
    	at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.setupJob(FileOutputCommitter.java:356)
    	at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.setupJob(HadoopMapReduceCommitProtocol.scala:188)
    	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:269)
    	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
    	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
    	at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
    	at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
    	at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
    	at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
    	at org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.$anonfun$executeCollect$1(AdaptiveSparkPlanExec.scala:374)
    	at org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.withFinalPlanUpdate(AdaptiveSparkPlanExec.scala:402)
    	at org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.executeCollect(AdaptiveSparkPlanExec.scala:374)
    	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
    	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
    	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
    	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
    	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
    	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
    	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
    	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
    	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
    	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
    	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
    	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
    	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
    	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
    	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
    	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
    	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
    	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
    	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
    	at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:859)
    	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:388)
    	at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:361)
    	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:240)
    	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:75)
    	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:52)
    	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
    	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
    	at py4j.Gateway.invoke(Gateway.java:282)
    	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
    	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
    	at java.base/java.lang.Thread.run(Thread.java:1583)
    Caused by: java.io.FileNotFoundException: Hadoop bin directory does not exist: C:\Users\scl\tools\hadoop-3.2.0\bin -see https://wiki.apache.org/hadoop/WindowsProblems
    	at org.apache.hadoop.util.Shell.getQualifiedBinInner(Shell.java:607)
    	at org.apache.hadoop.util.Shell.getQualifiedBin(Shell.java:591)
    	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:688)
    	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:79)
    	at org.apache.hadoop.conf.Configuration.getTimeDurationHelper(Configuration.java:1907)
    	at org.apache.hadoop.conf.Configuration.getTimeDuration(Configuration.java:1867)
    	at org.apache.hadoop.conf.Configuration.getTimeDuration(Configuration.java:1840)
    	at org.apache.hadoop.util.ShutdownHookManager.getShutdownTimeout(ShutdownHookManager.java:183)
    	at org.apache.hadoop.util.ShutdownHookManager$HookEntry.<init>(ShutdownHookManager.java:207)
    	at org.apache.hadoop.util.ShutdownHookManager.addShutdownHook(ShutdownHookManager.java:304)
    	at org.apache.spark.util.SparkShutdownHookManager.install(ShutdownHookManager.scala:181)
    	at org.apache.spark.util.ShutdownHookManager$.shutdownHooks$lzycompute(ShutdownHookManager.scala:50)
    	at org.apache.spark.util.ShutdownHookManager$.shutdownHooks(ShutdownHookManager.scala:48)
    	at org.apache.spark.util.ShutdownHookManager$.addShutdownHook(ShutdownHookManager.scala:153)
    	at org.apache.spark.util.ShutdownHookManager$.<init>(ShutdownHookManager.scala:58)
    	at org.apache.spark.util.ShutdownHookManager$.<clinit>(ShutdownHookManager.scala)
    	at org.apache.spark.util.Utils$.createTempDir(Utils.scala:242)
    	at org.apache.spark.util.SparkFileUtils.createTempDir(SparkFileUtils.scala:103)
    	at org.apache.spark.util.SparkFileUtils.createTempDir$(SparkFileUtils.scala:102)
    	at org.apache.spark.util.Utils$.createTempDir(Utils.scala:94)
    	at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:372)
    	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:964)
    	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:194)
    	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:217)
    	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
    	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1120)
    	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1129)
    	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)




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
