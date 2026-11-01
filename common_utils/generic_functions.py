import pyspark.sql.functions as f 
from common_utils.logging import get_logger
logger = get_logger("generic_functions")

logger = get_logger("generic_functions")

def rename_columns(df,rename):
    """
    Rename columns from a dictionary.
    Missing columns are ignored.
    """
    for old_name, new_name in rename.items():
        if old_name in df.columns:
         logger.info("renaming columns %s to %s",old_name , new_name)
         df = df.withColumnRenamed(old_name,new_name)

    return df
        
    

def drop_columns(df, columns_names):
    #handle single string
    if isinstance(columns_names, str):
        columns_names = [columns_names]
    #if the column name is single string it will make sure column name is always a list
    valid_cols = [c for c in df.columns if c in columns_names]
    # if will get the list of columns that i want to drop and will verify they are existed in the dataframe
    if valid_cols:
        df = df.drop(*valid_cols)

    #after verifying if there is a column in valid records it will drop it
    return df

def casting(df,column_names):
    """
    Safely cast columns using try_cast.

    For integral data types, first cast to DOUBLE
    so values such as '34.0' can become 34.
    """
    INTEGRAL_TYPES = {
    "byte",
    "short",
    "int",
    "integer",
    "long" }

    for column, target_type in column_names.items():
        if column not in df.columns:
            logger.warning("Columns [%s] does not exist in the dataframe", column)
            continue
        logger.info("casting columns [%s] to [%s]", column, target_type)
        if target_type.lower() in INTEGRAL_TYPES:
           expression = (
               f"try_cast("
                f"try_cast(`{column}` AS DOUBLE) "
                f"AS {target_type})"
               
           )
        else:
          expression = (
                f"try_cast(`{column}` AS {target_type})"
            )
          
        df = df.withColumn(column, f.expr(expression))
    return df



