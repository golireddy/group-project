
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import trim
def dataquailty(df,id,distinct_columns):

    # Check Duplicates in ID
    df.groupBy(id).count().filter('count>1').display()

    #Null Value Checks
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    # DATA TYPE CHECKS
    df.printSchema()
    df=df.withColumn(id,col(id).cast('int'))
    df.printSchema()

    # Check unique shrinkage_type
    for c in distinct_columns:
         df.select(c).distinct().display()

    #check empty values
    string_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for c in string_columns:

        empty_count = df.filter(
            col(c).isNotNull() &
            (trim(col(c)) == "")
        ).count()

        print(f"{c}: {empty_count} empty values")

    # Check each string column for extra spaces
    for c in string_columns:

        print(f"Checking extra spaces in: {c}")

        df.filter(col(c).isNotNull() &(col(c) != trim(col(c)))).display()
