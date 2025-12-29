import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, max as spark_max
from delta.tables import DeltaTable


def get_last_watermark(spark, target_table):
    """
    Returns the last processed updated_at timestamp
    from the target Delta table.
    """
    if spark.catalog.tableExists(target_table):
        return spark.table(target_table).select(spark_max("updated_at").alias("wm")).collect()[0]["wm"]
    return None


def read_incremental_from_sql_server(spark, jdbc_url, props, last_wm):
    """
    Reads only new or updated records from SQL Server
    based on updated_at watermark. Test 1
    """
    if last_wm:
        query = f"""
        (SELECT *
         FROM dbo.Customer
         WHERE updatedAt > '{last_wm}') AS src
        """
    else:
        # First run → full load
        query = "dbo.Customer"

    return spark.read.jdbc(url=jdbc_url, table=query, properties=props)


def merge_into_delta(spark, df, target_table):
    """
    Upserts data into Delta table using MERGE.
    """
    if spark.catalog.tableExists(target_table):
        delta_tbl = DeltaTable.forName(spark, target_table)

        (
            delta_tbl.alias("t")
            .merge(df.alias("s"), "t.CustomerID = s.CustomerID")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").saveAsTable(target_table)


def main():
    spark = SparkSession.builder.getOrCreate()

    # ---------------------------
    # Bundle variables (env-specific)
    # ---------------------------
    catalog = os.environ["DATABRICKS_BUNDLE_VARIABLE_CATALOG"]
    schema = os.environ["DATABRICKS_BUNDLE_VARIABLE_SCHEMA"]
    target_table = f"{catalog}.{schema}.customer2"

    # ---------------------------
    # Secrets
    # ---------------------------
    sql_host = dbutils.secrets.get("dab-sql-server", "sql_host")
    sql_user = dbutils.secrets.get("dab-sql-server", "sql_user")
    sql_password = dbutils.secrets.get("dab-sql-server", "sql_password")
    sql_database = dbutils.secrets.get("dab-sql-server", "sql_database")

    jdbc_url = f"jdbc:sqlserver://{sql_host}:1433;" f"databaseName={sql_database}"

    connection_properties = {
        "user": sql_user,
        "password": sql_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    # ---------------------------
    # Incremental logic
    # ---------------------------
    last_wm = get_last_watermark(spark, target_table)

    source_df = read_incremental_from_sql_server(spark, jdbc_url, connection_properties, last_wm)

    # If no new data, exit early (important optimization)
    if source_df.rdd.isEmpty():
        print("✅ No new or updated records found. Skipping write.")
        return

    # Add audit columns
    final_df = source_df.withColumn("ingestion_ts", current_timestamp()).withColumn("source_system", lit("sql_server"))

    merge_into_delta(spark, final_df, target_table)

    print(f"✅ Incremental load completed for {target_table}")


if __name__ == "__main__":
    main()
