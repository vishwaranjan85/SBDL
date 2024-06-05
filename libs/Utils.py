from pyspark.sql import SparkSession

def get_spark_session(env):
    if env == "LOCAL":
        return (SparkSession.builder
                .config('spark.drive.extraJavaOptions', '-Dlog4j.configuration=file:log4.properties')
                .master("local[2]").enableHiveSupport().getOrCreate())
    else:
        return SparkSession.builder.enableHiveSupport().getOrCreate()
