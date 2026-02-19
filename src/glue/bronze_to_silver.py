"""
Bronze to Silver Layer Transformation
Cleans and validates data, creates Delta Lake tables
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
from pyspark.sql.window import Window

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'silver_bucket',
    'database_name'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Delta Lake configuration is set via job parameters (--conf and --extra-jars)
# Static configs cannot be modified after Spark session is initialized

# Parameters with validation
BRONZE_BUCKET = args.get('bronze_bucket', '')
SILVER_BUCKET = args.get('silver_bucket', '')
DATABASE_NAME = args.get('database_name', '')

# Validate required parameters
if not BRONZE_BUCKET:
    raise ValueError("Missing required parameter: --bronze_bucket")
if not SILVER_BUCKET:
    raise ValueError("Missing required parameter: --silver_bucket")
if not DATABASE_NAME:
    raise ValueError("Missing required parameter: --database_name")

print(f"Configuration:")
print(f"  Bronze Bucket: {BRONZE_BUCKET}")
print(f"  Silver Bucket: {SILVER_BUCKET}")
print(f"  Database Name: {DATABASE_NAME}")
print("")


def process_calendly_events():
    """
    Process Calendly events from Bronze to Silver layer
    """
    print("Processing Calendly events...")

    # Read from Bronze layer
    bronze_path = f"s3://{BRONZE_BUCKET}/calendly_events/"

    try:
        # Read JSON files from Bronze
        df_events = spark.read.json(bronze_path, multiLine=True)

        # Data cleaning and validation
        df_clean = clean_calendly_events(df_events)

        # Write to Silver layer as Delta table
        silver_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"

        print(f"DEBUG: About to write to silver_path: '{silver_path}'")
        print(f"DEBUG: SILVER_BUCKET value: '{SILVER_BUCKET}'")

        if not silver_path or silver_path == "s3:///calendly_events_silver/":
            raise ValueError(f"Invalid silver_path constructed: '{silver_path}' (SILVER_BUCKET='{SILVER_BUCKET}')")

        # Upsert into Delta table (merge on invitee_uri)
        if DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)

            delta_table.alias("target").merge(
                df_clean.alias("source"),
                "target.invitee_uri = source.invitee_uri"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            print(f"Merged into existing Delta table: {silver_path}")
        else:
            # Create new Delta table
            df_clean.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("booking_year", "booking_month", "marketing_channel") \
                .save(silver_path)

            print(f"Created new Delta table: {silver_path}")

        # Register as table in Glue Catalog (if database name provided)
        if DATABASE_NAME:
            try:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.calendly_events_silver
                    USING DELTA
                    LOCATION '{silver_path}'
                """)
                print(f"Registered table in Glue Catalog: {DATABASE_NAME}.calendly_events_silver")
            except Exception as catalog_error:
                print(f"Warning: Could not register table in Glue Catalog: {str(catalog_error)}")
                print("Data was still written successfully to Silver layer")

        record_count = df_clean.count()
        print(f"Processed {record_count} Calendly event records")

        return record_count

    except Exception as e:
        print(f"Error processing Calendly events: {str(e)}")
        raise


def clean_calendly_events(df: DataFrame) -> DataFrame:
    """
    Clean and validate Calendly events data
    Extract nested fields from payload struct

    Args:
        df: Raw DataFrame from Bronze layer with parsed JSON struct

    Returns:
        Cleaned DataFrame
    """
    from pyspark.sql.functions import col, current_timestamp, substring, regexp_extract, dayofweek, hour

    # Access nested struct fields directly (payload is already parsed as struct)
    df_parsed = df.select(
        # Top-level fields
        col("webhook_event"),
        col("webhook_received_at").cast(TimestampType()),
        col("marketing_channel"),
        col("event_type_url"),

        # Nested payload fields (using struct notation)
        col("payload.payload.uri").alias("invitee_uri"),
        col("payload.payload.email").alias("invitee_email"),
        col("payload.payload.name").alias("invitee_name"),
        col("payload.payload.status").alias("invitee_status"),
        col("payload.payload.timezone").alias("invitee_timezone"),
        col("payload.payload.text_reminder_number").alias("invitee_phone"),
        col("payload.payload.created_at").cast(TimestampType()).alias("invitee_created_at"),
        col("payload.payload.updated_at").cast(TimestampType()).alias("invitee_updated_at"),
        col("payload.payload.canceled").alias("invitee_canceled"),
        col("payload.payload.rescheduled").alias("invitee_rescheduled"),

        # Scheduled event details
        col("payload.payload.scheduled_event.name").alias("event_name"),
        col("payload.payload.scheduled_event.status").alias("event_status"),
        col("payload.payload.scheduled_event.start_time").cast(TimestampType()).alias("event_start_time"),
        col("payload.payload.scheduled_event.end_time").cast(TimestampType()).alias("event_end_time"),
        col("payload.payload.scheduled_event.created_at").cast(TimestampType()).alias("event_created_at"),
        col("payload.payload.scheduled_event.event_type").alias("event_type"),

        # Tracking info
        col("payload.payload.tracking.utm_campaign").alias("utm_campaign"),
        col("payload.payload.tracking.utm_source").alias("utm_source"),
        col("payload.payload.tracking.utm_medium").alias("utm_medium"),
        col("payload.payload.tracking.utm_content").alias("utm_content"),
        col("payload.payload.tracking.utm_term").alias("utm_term"),

        # Location info
        col("payload.payload.scheduled_event.location.type").alias("location_type"),
        col("payload.payload.scheduled_event.location.join_url").alias("location_join_url"),

        # Metadata
        current_timestamp().alias("processed_at")
    )

    # Add derived columns
    df_clean = df_parsed.withColumn(
        "invitee_first_name",
        regexp_extract(col("invitee_name"), "^(\\S+)", 1)
    ).withColumn(
        "invitee_last_name",
        regexp_extract(col("invitee_name"), "\\s+(\\S+)$", 1)
    ).withColumn(
        "booking_date",
        col("event_start_time").cast("date")
    ).withColumn(
        "booking_year",
        substring(col("booking_date").cast("string"), 1, 4)
    ).withColumn(
        "booking_month",
        substring(col("booking_date").cast("string"), 6, 2)
    ).withColumn(
        "booking_day_of_week",
        dayofweek(col("event_start_time"))
    ).withColumn(
        "booking_hour",
        hour(col("event_start_time"))
    )

    # Filter out null invitee_uri (invalid records)
    df_clean = df_clean.filter(col("invitee_uri").isNotNull())

    return df_clean


def process_marketing_spend():
    """
    Process marketing spend data from Bronze to Silver layer
    """
    print("Processing marketing spend data...")

    # Read from Bronze layer
    bronze_path = f"s3://{BRONZE_BUCKET}/marketing_spend/"

    try:
        # Read JSON files
        df_spend = spark.read.json(bronze_path, multiLine=True)

        # Flatten the records array if nested
        if "records" in df_spend.columns:
            df_spend = df_spend.select(explode("records").alias("record")) \
                .select("record.*")

        # Clean and validate
        df_clean = clean_marketing_spend(df_spend)

        # Write to Silver layer as Delta table
        silver_path = f"s3://{SILVER_BUCKET}/marketing_spend_silver/"

        # Upsert into Delta table
        if DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)

            delta_table.alias("target").merge(
                df_clean.alias("source"),
                "target.date = source.date AND target.channel = source.channel"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            print(f"Merged into existing Delta table: {silver_path}")
        else:
            df_clean.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("year", "month", "channel") \
                .save(silver_path)

            print(f"Created new Delta table: {silver_path}")

        # Register as table in Glue Catalog (if database name provided)
        if DATABASE_NAME:
            try:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.marketing_spend_silver
                    USING DELTA
                    LOCATION '{silver_path}'
                """)
                print(f"Registered table in Glue Catalog: {DATABASE_NAME}.marketing_spend_silver")
            except Exception as catalog_error:
                print(f"Warning: Could not register table in Glue Catalog: {str(catalog_error)}")
                print("Data was still written successfully to Silver layer")

        record_count = df_clean.count()
        print(f"Processed {record_count} marketing spend records")

        return record_count

    except Exception as e:
        print(f"Error processing marketing spend: {str(e)}")
        raise


def clean_marketing_spend(df: DataFrame) -> DataFrame:
    """
    Clean and validate marketing spend data

    Args:
        df: Raw DataFrame from Bronze layer

    Returns:
        Cleaned DataFrame
    """
    df_clean = df.select(
        col("date").cast(DateType()),
        col("channel"),
        col("spend").cast(DoubleType()),
        col("ingested_at").cast(TimestampType()),
        col("source_file"),
        col("ingestion_date").cast(DateType())
    )

    # Extract date partitions
    df_clean = df_clean.withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))

    # Data quality checks
    df_clean = df_clean.filter(
        col("date").isNotNull() &
        col("channel").isNotNull() &
        col("spend").isNotNull() &
        (col("spend") >= 0)
    )

    # Remove duplicates (keep latest ingestion)
    window_spec = Window.partitionBy("date", "channel").orderBy(col("ingested_at").desc())
    df_clean = df_clean.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Add processing metadata
    df_clean = df_clean.withColumn("silver_processing_timestamp", current_timestamp())

    return df_clean


def main():
    """
    Main ETL process
    """
    print(f"Starting Bronze to Silver transformation...")
    print(f"Bronze bucket: {BRONZE_BUCKET}")
    print(f"Silver bucket: {SILVER_BUCKET}")
    print(f"Database: {DATABASE_NAME}")

    # Process both datasets
    events_count = process_calendly_events()
    spend_count = process_marketing_spend()

    print(f"Bronze to Silver transformation completed successfully")
    print(f"Total events processed: {events_count}")
    print(f"Total spend records processed: {spend_count}")

    job.commit()


if __name__ == "__main__":
    main()
