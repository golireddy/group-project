from delta.tables import DeltaTable
from pyspark.sql import functions as F
from functools import reduce
from operator import and_, or_

from common_utils.logging import get_logger

logger = get_logger("scd_functions")


def scd_type2(
    spark,
    source,
    target_table,
    key_columns,
    tracked_columns=None
):
    """
    Perform SCD Type 2.

    Existing changed records are expired.
    New versions are inserted.
    Completely new records are inserted.

    Parameters:
        spark: SparkSession
        source: Source DataFrame
        target_table: Fully qualified Delta table
        key_columns: Columns used to identify records
        tracked_columns: Business columns to compare for changes
    """

    logger.info("Starting SCD Type 2 for %s", target_table)

    # ------------------------------------------------
    # 1. Validate key columns
    # ------------------------------------------------

    if not key_columns:
        raise ValueError("At least one key column is required")

    missing_keys = [
        k for k in key_columns
        if k not in source.columns
    ]

    if missing_keys:
        raise ValueError(f"Missing key columns: {missing_keys}")

    # Check for null keys
    null_keys = reduce(
        or_,
        [F.col(k).isNull() for k in key_columns]
    )

    if source.filter(null_keys).limit(1).count():
        raise ValueError("Source contains null merge keys")

    # Check for duplicate source keys
    duplicates = (
        source.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count():
        raise ValueError("Source contains duplicate merge keys")

    # ------------------------------------------------
    # 2. Identify business columns to track
    # ------------------------------------------------

    if tracked_columns is None:
        tracked_columns = [
            c for c in source.columns
            if c not in key_columns
        ]

    if not tracked_columns:
        raise ValueError("At least one tracked column is required")

    missing_tracked = [
        c for c in tracked_columns
        if c not in source.columns
    ]

    if missing_tracked:
        raise ValueError(
            f"Missing tracked columns: {missing_tracked}"
        )

    # SCD metadata columns
    valid_from_col = "valid_from"
    valid_to_col = "valid_to"
    current_flag_col = "is_current"

    metadata_columns = [
        valid_from_col,
        valid_to_col,
        current_flag_col
    ]

    collisions = [
        c for c in metadata_columns
        if c in source.columns
    ]

    if collisions:
        raise ValueError(
            f"Source already contains SCD metadata columns: {collisions}"
        )

    # Use one timestamp for both operations
    run_ts = spark.sql(
        "SELECT current_timestamp() AS run_ts"
    ).first()["run_ts"]

    # ------------------------------------------------
    # 3. Check whether the target table exists
    # ------------------------------------------------

    if not spark.catalog.tableExists(target_table):

        logger.info(
            "Target table does not exist. Performing initial load."
        )

        initial_load = (
            source
            .withColumn(
                valid_from_col,
                F.lit(run_ts).cast("timestamp")
            )
            .withColumn(
                valid_to_col,
                F.to_timestamp(
                    F.lit("9999-12-31 23:59:59")
                )
            )
            .withColumn(
                current_flag_col,
                F.lit(True)
            )
        )

        (
            initial_load.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(target_table)
        )

        return True

    # ------------------------------------------------
    # 4. Get current records from target
    # ------------------------------------------------

    target_df = spark.table(target_table)

    required_columns = source.columns + metadata_columns

    missing_target_columns = [
        c for c in required_columns
        if c not in target_df.columns
    ]

    if missing_target_columns:
        raise ValueError(
            f"Missing target columns: {missing_target_columns}"
        )

    current_target = target_df.filter(
        F.col(current_flag_col) == True
    )

    # Make sure each key has only one current record
    current_duplicates = (
        current_target.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    if current_duplicates.limit(1).count():
        raise ValueError(
            "Target contains multiple current records for the same key"
        )

    # ------------------------------------------------
    # 5. Add effective timestamp to source
    # ------------------------------------------------

    temp_col = "__scd2_effective_ts"

    if temp_col in source.columns:
        raise ValueError(f"Reserved source column: {temp_col}")

    staged_source = source.withColumn(
        temp_col,
        F.lit(run_ts).cast("timestamp")
    )

    # ------------------------------------------------
    # 6. Build key and change conditions
    # ------------------------------------------------

    key_condition = reduce(
        and_,
        [
            F.col(f"s.`{k}`") == F.col(f"t.`{k}`")
            for k in key_columns
        ]
    )

    change_condition = reduce(
        or_,
        [
            ~F.col(f"s.`{c}`").eqNullSafe(F.col(f"t.`{c}`"))
            for c in tracked_columns
        ]
    )

    # ------------------------------------------------
    # 7. Identify changed records
    # ------------------------------------------------

    changed_source = (
        staged_source.alias("s")
        .join(
            current_target.alias("t"),
            key_condition & change_condition,
            "inner"
        )
        .select("s.*")
    )

    # ------------------------------------------------
    # 8. Identify completely new records
    # ------------------------------------------------

    new_source = (
        staged_source.alias("s")
        .join(
            current_target.alias("t"),
            key_condition,
            "left_anti"
        )
        .select("s.*")
    )

    # ------------------------------------------------
    # 9. Expire existing records
    # ------------------------------------------------

    merge_condition = " AND ".join(
        [
            f"t.`{k}` = s.`{k}`"
            for k in key_columns
        ]
    )

    merge_condition += f" AND t.`{current_flag_col}` = true"

    if changed_source.limit(1).count():

        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(
                changed_source.alias("s"),
                merge_condition
            )
            .whenMatchedUpdate(
                set={
                    valid_to_col: f"s.`{temp_col}`",
                    current_flag_col: "false"
                }
            )
            .execute()
        )

    # ------------------------------------------------
    # 10. Insert new versions and new records
    # ------------------------------------------------

    records_to_insert = changed_source.unionByName(new_source)

    insert_values = {
        c: f"s.`{c}`"
        for c in source.columns
    }

    insert_values.update({
        valid_from_col: f"s.`{temp_col}`",
        valid_to_col: "TIMESTAMP '9999-12-31 23:59:59'",
        current_flag_col: "true"
    })

    if records_to_insert.limit(1).count():

        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(
                records_to_insert.alias("s"),
                merge_condition
            )
            .whenNotMatchedInsert(
                values=insert_values
            )
            .execute()
        )

    logger.info(
        "SCD Type 2 completed for %s",
        target_table
    )

    return True