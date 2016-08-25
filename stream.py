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
                StructField('device_id', StringType(), True),
                StructField('steps', IntegerType(), True),
                StructField('heartbeat', IntegerType(), True),
                StructField('temperature', FloatType(), True),
                StructField('battery_level', IntegerType(), True),
                StructField('calories_spent', IntegerType(), True),
                StructField('distance', FloatType(), True),
                StructField('current_time', IntegerType(), True),
                StructField('bcc', IntegerType(), True)
            ])
            # df
        except Exception as e:
            print(e)
            pass


    py_rdd.foreachRDD(process)
    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()
    spark_streaming_context.stop()
