export AWS_ACCESS_KEY_ID=""
export AWS_ACCESS_KEY=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_SECRET_KEY=""

../spark-1.6.2/bin/spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.10:1.6.1,com.databricks:spark-redshift_2.10:0.6.0 --jars RedshiftJDBC41-1.1.10.1010.jar stream.py
