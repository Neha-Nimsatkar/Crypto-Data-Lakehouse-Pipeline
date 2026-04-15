Microsoft Windows [Version 10.0.26200.8037]
(c) Microsoft Corporation. All rights reserved.

A:\Crypto-Data-lakehouse-pipeline>a:\Crypto-Data-lakehouse-pipeline\venv\Scripts\activate.bat

(venv) A:\Crypto-Data-lakehouse-pipeline>python streaming_pipeline/medallion/gold/gold_transforamtions.py
:: loading settings :: url = jar:file:/A:/Crypto-Data-lakehouse-pipeline/venv/Lib/site-packages/pyspark/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: C:\Users\saksh\.ivy2\cache
The jars for the packages stored in: C:\Users\saksh\.ivy2\jars
io.delta#delta-core_2.12 added as a dependency
org.apache.hadoop#hadoop-aws added as a dependency
com.amazonaws#aws-java-sdk-bundle added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-3b57deb0-8ac9-4d86-b1cc-30070965caef;1.0
        confs: [default]
        found io.delta#delta-core_2.12;2.4.0 in central
        found io.delta#delta-storage;2.4.0 in central
        found org.antlr#antlr4-runtime;4.9.3 in central
        found org.apache.hadoop#hadoop-aws;3.3.4 in central
        found com.amazonaws#aws-java-sdk-bundle;1.12.262 in central
        found org.wildfly.openssl#wildfly-openssl;1.0.7.Final in central
:: resolution report :: resolve 474ms :: artifacts dl 25ms
        :: modules in use:
        com.amazonaws#aws-java-sdk-bundle;1.12.262 from central in [default]
        io.delta#delta-core_2.12;2.4.0 from central in [default]
        io.delta#delta-storage;2.4.0 from central in [default]
        org.antlr#antlr4-runtime;4.9.3 from central in [default]
        org.apache.hadoop#hadoop-aws;3.3.4 from central in [default]
        org.wildfly.openssl#wildfly-openssl;1.0.7.Final from central in [default]
        :: evicted modules:
        com.amazonaws#aws-java-sdk-bundle;1.12.200 by [com.amazonaws#aws-java-sdk-bundle;1.12.262] in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   7   |   0   |   0   |   1   ||   6   |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-3b57deb0-8ac9-4d86-b1cc-30070965caef
        confs: [default]
        0 artifacts copied, 6 already retrieved (0kB/22ms)
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/04/15 09:07:07 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
26/04/15 09:07:07 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
26/04/15 09:07:16 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
[Stage 0:>                                    [Stage 0:=======>                             [Stage 0:=====================================                                              26/04/15 09:07:35 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
26/04/15 09:07:40 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
26/04/15 09:07:43 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
🏆 Gold Streams running! Writing trends, performance & snapshot to s3a://crypto-lakehouse-neha/gold/
26/04/15 09:07:46 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
[Stage 6:>                                    [Stage 6:>                  (0 + 2) / 2][Stage[Stage 6:>                  (0 + 2) / 2][Stage                                              [Stage 9:=======>                             [Stage 9:=====================================[Stage 7:>                (0 + 11) / 50][Stage[Stage 7:>                                    [Stage 7:>                (0 + 12) / 50][Stage[Stage 7:>                (0 + 14) / 50][Stage[Stage 7:=====>          (18 + 12) / 50][Stage[Stage 7:=>(25 + 12) / 50][Stage 10:>  (0 + 0)[Stage 7:=>(38 + 12) / 50][Stage 10:>  (0 + 0)[Stage 7:==>(48 + 2) / 50][Stage 10:> (0 + 10)                                              [Stage 10:===========>   (37 + 13) / 50][Stage                                              [Stage 12:===========>   (38 + 12) / 50][Stage                                              26/04/15 09:08:00 ERROR MicroBatchExecution: Query [id = 15d7ec79-bf35-402c-b098-4e77e4777edc, runId = f3dac9e1-2805-4b8c-9178-f2d05c2c8268] terminated with error
org.apache.spark.sql.AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: 641dc5b5-029d-4502-8e4f-372b04253f7c).
To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
'.option("mergeSchema", "true")'.
For other operations, set the session configuration
spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
specific to the operation for details.

Table schema:
root
-- coin_id: string (nullable = true)
-- event_timestamp: timestamp (nullable = true)
-- price_usd: double (nullable = true)
-- moving_avg_price: double (nullable = true)
-- price_volatility: double (nullable = true)
-- market_cap_rank: integer (nullable = true)


Data schema:
root
-- coin_id: string (nullable = true)
-- start_time: timestamp (nullable = true)
-- moving_avg_price: double (nullable = true)
-- price_volatility: double (nullable = true)

         
        at org.apache.spark.sql.delta.MetadataMismatchErrorBuilder.finalizeAndThrow(DeltaErrors.scala:2961)
        at org.apache.spark.sql.delta.schema.ImplicitMetadataOperation.updateMetadata(ImplicitMetadataOperation.scala:130)
        at org.apache.spark.sql.delta.schema.ImplicitMetadataOperation.updateMetadata$(ImplicitMetadataOperation.scala:52)
        at org.apache.spark.sql.delta.sources.DeltaSink.updateMetadata(DeltaSink.scala:40)
        at org.apache.spark.sql.delta.sources.DeltaSink.addBatch(DeltaSink.scala:109)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$17(MicroBatchExecution.scala:729)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:118)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:195)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:103)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$16(MicroBatchExecution.scala:726)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:411)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:409)
        at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runBatch(MicroBatchExecution.scala:726)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:284)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:411)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:409)
        at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:247)
        at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:237)
        at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:306)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:284)
        at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:207)
26/04/15 09:08:00 ERROR MicroBatchExecution: Query [id = 82c7d90a-13b4-4313-8d95-035251d2a588, runId = 5ac210fd-ca20-46df-8699-571c316456df] terminated with error
org.apache.spark.sql.AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: c8f95e76-cfa0-46b1-89c5-3477a9451bab).
To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
'.option("mergeSchema", "true")'.
For other operations, set the session configuration
spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
specific to the operation for details.

Table schema:
root
-- coin_id: string (nullable = true)
-- date: date (nullable = true)
-- daily_avg_price: double (nullable = true)
-- daily_max_price: double (nullable = true)
-- daily_min_price: double (nullable = true)
-- daily_avg_volume: double (nullable = true)
-- load_timestamp: timestamp (nullable = true)


Data schema:
root
-- coin_id: string (nullable = true)
-- window_start: timestamp (nullable = true)
-- daily_avg_price: double (nullable = true)
-- daily_max_price: double (nullable = true)
-- daily_min_price: double (nullable = true)
-- daily_avg_volume: double (nullable = true)

         
        at org.apache.spark.sql.delta.MetadataMismatchErrorBuilder.finalizeAndThrow(DeltaErrors.scala:2961)
        at org.apache.spark.sql.delta.schema.ImplicitMetadataOperation.updateMetadata(ImplicitMetadataOperation.scala:130)
        at org.apache.spark.sql.delta.schema.ImplicitMetadataOperation.updateMetadata$(ImplicitMetadataOperation.scala:52)
        at org.apache.spark.sql.delta.sources.DeltaSink.updateMetadata(DeltaSink.scala:40)
        at org.apache.spark.sql.delta.sources.DeltaSink.addBatch(DeltaSink.scala:109)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$17(MicroBatchExecution.scala:729)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:118)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:195)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:103)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$16(MicroBatchExecution.scala:726)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:411)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:409)
        at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runBatch(MicroBatchExecution.scala:726)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:284)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:411)
        at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:409)
        at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:247)
        at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
        at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:237)
        at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:306)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:827)
        at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:284)
        at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:207)
Traceback (most recent call last):
  File "A:\Crypto-Data-lakehouse-pipeline\streaming_pipeline\medallion\gold\gold_transforamtions.py", line 109, in <module>
    spark.streams.awaitAnyTermination()
  File "a:\Crypto-Data-lakehouse-pipeline\venv\Lib\site-packages\pyspark\sql\streaming\query.py", line 535, in awaitAnyTermination
    return self._jsqm.awaitAnyTermination()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "a:\Crypto-Data-lakehouse-pipeline\venv\Lib\site-packages\py4j\java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
                   ^^^^^^^^^^^^^^^^^
  File "a:\Crypto-Data-lakehouse-pipeline\venv\Lib\site-packages\pyspark\errors\exceptions\captured.py", line 175, in deco
    raise converted from None
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED] Query [id = 82c7d90a-13b4-4313-8d95-035251d2a588, runId = 5ac210fd-ca20-46df-8699-571c316456df] terminated with exception: A schema mismatch detected when writing to the Delta table (Table ID: c8f95e76-cfa0-46b1-89c5-3477a9451bab).
To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
'.option("mergeSchema", "true")'.
For other operations, set the session configuration
spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
specific to the operation for details.

Table schema:
root
-- coin_id: string (nullable = true)
-- date: date (nullable = true)
-- daily_avg_price: double (nullable = true)
-- daily_max_price: double (nullable = true)
-- daily_min_price: double (nullable = true)
-- daily_avg_volume: double (nullable = true)
-- load_timestamp: timestamp (nullable = true)


Data schema:
root
-- coin_id: string (nullable = true)
-- window_start: timestamp (nullable = true)
-- daily_avg_price: double (nullable = true)
-- daily_max_price: double (nullable = true)
-- daily_min_price: double (nullable = true)
-- daily_avg_volume: double (nullable = true)

         

(venv) A:\Crypto-Data-lakehouse-pipeline>SUCCESS: The process with PID 19656 (child process of PID 22032) has been terminated.
SUCCESS: The process with PID 22032 (child process of PID 6824) has been terminated.
SUCCESS: The process with PID 6824 (child process of PID 27040) has been terminated.