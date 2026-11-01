
import pyspark.sql.functions as F
from common_utils.logging import get_logger

logger = get_logger('common_utils_bronze')

def read_raw(spark , raw_path , file_format, options = None):
    '''
    Read Raw files from volume

    Raw_path : path of the raw files
    Options : key value
    returns a dataframe
    '''

    logger.info('Reading from path : %s', raw_path)
    logger.info("File format: %s", file_format)
    reader = spark.read.format(file_format)

    for key, value in(options or {}).items():
        reader = reader.option(key, value)

    return reader.load(raw_path)
    logger.info("Read is completed and the dataframe is stored")



def bronze_ingestor(df , mode , target_table):
    ''' Add Audit Columns and write in Bronze  as delta table

    df: Dataframe read from raw volumn
    target_table : catalog.schema.table
    mode: overwrite or append

    Return the no. of rows written
    '''
    logger.info("Adding Audit column into the Dataframe")

    df = df.withColumn('last_update_timestamp', F.current_timestamp())\
        .withColumn('file_path', F.col('_metadata.file_path'))

    logger.info('writing dalta table : %s', target_table)
    logger.info("Selected mode: %s", mode)


    df.write.format('delta').mode(mode).option('overwriteSchema', 'true').saveAsTable(target_table)
    return df.count()

