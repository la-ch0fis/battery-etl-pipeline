import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "raw_bucket",
    "processed_bucket",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

RAW_BUCKET = args["raw_bucket"]
PROCESSED_BUCKET = args["processed_bucket"]

battery_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3://{RAW_BUCKET}/battery14_df.csv")
)

degrees_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3://{RAW_BUCKET}/degrees.csv")
)

# Ensure consistent numeric types for join and filter
battery_df = (
    battery_df
    .withColumn("age", F.col("age").cast(DoubleType()))
    .withColumn("raw_score", F.col("raw_score").cast(DoubleType()))
    .withColumn("education_level", F.col("education_level").cast(IntegerType()))
)

degrees_df = degrees_df.withColumn("education_level", F.col("education_level").cast(IntegerType()))

# Filter: females, age > 30, country US, education_level > 6 (higher than Master's = 6), raw_score > 300
filtered_df = battery_df.filter(
    (F.col("gender") == "f")
    & (F.col("age") > 30)
    & (F.col("country") == "US")
    & (F.col("education_level") > 6)
    & (F.col("raw_score") > 300)
)

# Transform gender codes to full labels
transformed_df = filtered_df.withColumn(
    "gender",
    F.when(F.col("gender") == "f", "Female")
     .when(F.col("gender") == "m", "Male")
     .otherwise(F.col("gender")),
)

# Join to resolve education level code → human-readable degree name
result_df = transformed_df.join(degrees_df, on="education_level", how="left")

row_count = result_df.count()
print(f"ETL complete — {row_count} rows written to s3://{PROCESSED_BUCKET}/transformed/")

result_df.write.mode("overwrite").parquet(f"s3://{PROCESSED_BUCKET}/transformed/")

job.commit()
