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

# Configure Delta Lake
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Parameters
BRONZE_BUCKET = args['bronze_bucket']
SILVER_BUCKET = args['silver_bucket']
DATABASE_NAME = args['database_name']


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
        
        # Register as table in Glue Catalog
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.calendly_events_silver
            USING DELTA
            LOCATION '{silver_path}'
        """)
        
        record_count = df_clean.count()
        print(f"Processed {record_count} Calendly event records")
        
        return record_count
        
    except Exception as e:
        print(f"Error processing Calendly events: {str(e)}")
        raise


def clean_calendly_events(df: DataFrame) -> DataFrame:
    """
    Clean and validate Calendly events data
    
    Args:
        df: Raw DataFrame from Bronze layer
        
    Returns:
        Cleaned DataFrame
    """
    # Define schema expectations
    df_clean = df.select(
        col("event_id"),
        col("kinesis_sequence_number"),
        col("processed_at").cast(TimestampType()),
        col("webhook_event"),
        col("webhook_received_at").cast(TimestampType()),
        col("webhook_created_at").cast(TimestampType()),
        
        # Marketing
        col("marketing_channel"),
        col("event_type_url"),
        
        # Invitee
        col("invitee_uri"),
        col("invitee_email"),
        col("invitee_name"),
        col("invitee_first_name"),
        col("invitee_last_name"),
        col("invitee_created_at").cast(TimestampType()),
        col("invitee_status"),
        col("invitee_timezone"),
        col("invitee_phone"),
        
        # Scheduled event
        col("scheduled_event_uri"),
        col("scheduled_event_name"),
        col("scheduled_event_start_time").cast(TimestampType()),
        col("scheduled_event_end_time").cast(TimestampType()),
        col("scheduled_event_created_at").cast(TimestampType()),
        col("scheduled_event_status"),
        col("scheduled_event_location_type"),
        col("scheduled_event_location"),
        
        # Host
        col("host_email"),
        col("host_name"),
        
        # UTM tracking
        col("utm_source"),
        col("utm_medium"),
        col("utm_campaign"),
        col("utm_content"),
        col("utm_term"),
        
        # Date partitions
        col("booking_date"),
        col("booking_year").cast(IntegerType()),
        col("booking_month").cast(IntegerType()),
        col("booking_day").cast(IntegerType()),
        col("booking_hour").cast(IntegerType()),
        col("booking_day_of_week")
    )
    
    # Data quality checks
    df_clean = df_clean.filter(
        col("invitee_uri").isNotNull() &
        col("scheduled_event_start_time").isNotNull() &
        col("marketing_channel").isNotNull()
    )
    
    # Remove duplicates based on invitee_uri (keep latest)
    window_spec = Window.partitionBy("invitee_uri").orderBy(col("processed_at").desc())
    df_clean = df_clean.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Add data quality metadata
    df_clean = df_clean.withColumn("silver_processing_timestamp", current_timestamp()) \
        .withColumn("data_quality_score", lit(1.0))
    
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
        
        # Register as table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.marketing_spend_silver
            USING DELTA
            LOCATION '{silver_path}'
        """)
        
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
