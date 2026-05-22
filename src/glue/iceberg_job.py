import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "processed_bucket",
    "glue_database",
    "iceberg_table",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

PROCESSED_BUCKET = args["processed_bucket"]
GLUE_DATABASE = args["glue_database"]
ICEBERG_TABLE = args["iceberg_table"]
df = spark.read.parquet(f"s3://{PROCESSED_BUCKET}/transformed/")

spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.`{GLUE_DATABASE}`")

# Write as Iceberg table — createOrReplace handles re-runs safely
(
    df.writeTo(f"glue_catalog.`{GLUE_DATABASE}`.`{ICEBERG_TABLE}`")
    .tableProperty("format-version", "2")
    .tableProperty("write.format.default", "parquet")
    .createOrReplace()
)

# Validate via SQL PySpark
total = spark.sql(
    f"SELECT COUNT(*) AS total FROM glue_catalog.`{GLUE_DATABASE}`.`{ICEBERG_TABLE}`"
).collect()[0]["total"]

sample = spark.sql(
    f"""
    SELECT user_id, age, gender, description, raw_score, country
    FROM glue_catalog.`{GLUE_DATABASE}`.`{ICEBERG_TABLE}`
    LIMIT 5
    """
)
sample.show(truncate=False)

print(f"Iceberg table '{GLUE_DATABASE}.{ICEBERG_TABLE}' ready — {total} rows.")

job.commit()
