from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import datetime
import json
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

aws_region = 'us-east-1'
kinesis_stream = 'stream_name'
kinesis_endpoint = 'https://kinesis.us-east-1.amazonaws.com/'
kinesis_app_name = 'app_name'
kinesis_initial_position = InitialPositionInStream.LATEST
kinesis_checkpoint_interval = 5
spark_batch_interval = 5



if __name__ == "__main__":
    spark_context = SparkContext(appName=kinesis_app_name)
    spark_streaming_context = StreamingContext(spark_context, spark_batch_interval)
    sql_context = SQLContext(spark_context)

    kinesis_stream = KinesisUtils.createStream(
        spark_streaming_context, kinesis_app_name, kinesis_stream, kinesis_endpoint,
        aws_region, kinesis_initial_position, kinesis_checkpoint_interval)

    kinesis_stream.pprint()
    py_rdd = kinesis_stream.map(lambda x: json.loads(x))

    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:

            sqlContext = getSqlContextInstance(rdd.context)
            schema = StructType([
                StructField('user_id', StringType(), True),
                StructField('username', StringType(), True),
                StructField('first_name', IntegerType(), True),
                StructField('surname', IntegerType(), True),
                StructField('age', FloatType(), True),
            ])
            df = sqlContext.createDataFrame(rdd, schema)
            df.registerTempTable("activity_log")
            df.write \
                .format("com.databricks.spark.redshift") \
                .option("url", "jdbc:redshiftURL.com:5439/database?user=USERNAME&password=PASSWORD") \
                .option("dbtable", "activity_log") \
                .option("tempdir", "s3n://spark-temp-data/") \
                .mode("append") \
                .save()
        except Exception as e:
            print(e)
            pass


    py_rdd.foreachRDD(process)
    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()
    spark_streaming_context.stop()
