from pyspark.sql import functions as F
def read_raw(spark,source_path):
    df=(spark.read\
            .option("header","true")\
            .option("inferSchema","false")\
            .csv(source_path)\
            .withColumn("source_file",F.col("_metadata.file_name"))\
            .withColumn("current_timestamp",F.current_timestamp())
    )
    return df

def write_table(df,target_table):
        ( df.write.format("delta")\
          .mode("overwrite")\
          .saveAsTable(target_table)
        )