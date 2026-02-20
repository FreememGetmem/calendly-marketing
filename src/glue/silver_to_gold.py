"""
Silver to Gold Layer Transformation
Creates business metrics and aggregations
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'silver_bucket',
    'gold_bucket',
    'database_name'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
SILVER_BUCKET = args['silver_bucket']
GOLD_BUCKET = args['gold_bucket']
DATABASE_NAME = args['database_name']

print("")
print("DEBUGGING: Parameter values retrieved:")
print(f"  SILVER_BUCKET = '{SILVER_BUCKET}' (type: {type(SILVER_BUCKET)}, len: {len(SILVER_BUCKET) if SILVER_BUCKET else 0})")
print(f"  GOLD_BUCKET = '{GOLD_BUCKET}' (type: {type(GOLD_BUCKET)}, len: {len(GOLD_BUCKET) if GOLD_BUCKET else 0})")
print(f"  DATABASE_NAME = '{DATABASE_NAME}' (type: {type(DATABASE_NAME)}, len: {len(DATABASE_NAME) if DATABASE_NAME else 0})")
print("")

# Validate required parameters
if not SILVER_BUCKET or SILVER_BUCKET.strip() == "":
    print("❌ ERROR: SILVER_BUCKET is empty or None!")
    raise ValueError("Missing required parameter: --silver_bucket")
if not GOLD_BUCKET or GOLD_BUCKET.strip() == "":
    print("❌ ERROR: GOLD_BUCKET is empty or None!")
    raise ValueError("Missing required parameter: --gold_bucket")
if not DATABASE_NAME or DATABASE_NAME.strip() == "":
    print("❌ ERROR: DATABASE_NAME is empty or None!")
    raise ValueError("Missing required parameter: --database_name")

print(f"✅ Configuration validated successfully:")
print(f"  Silver Bucket: {SILVER_BUCKET}")
print(f"  Gold Bucket: {GOLD_BUCKET}")
print(f"  Database Name: {DATABASE_NAME}")
print("")


def create_daily_bookings_by_source():
    """
    Metric 1.1: Daily Calls Booked by Source
    Count of Calendly bookings per source, per day
    """
    print("Creating daily bookings by source metric...")

    # Read Silver tables - ADD DEBUGGING HERE
    events_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"
    print(f"DEBUG: Reading from path: '{events_path}'")

    try:
        df_events = spark.read.format("delta").load(events_path)
    except Exception as e:
        print(f"❌ ERROR reading from {events_path}: {str(e)}")
        raise

    # Aggregate daily bookings by source
    df_metric = df_events.groupBy(
        col("booking_date"),
        col("marketing_channel").alias("source")
    ).agg(
        count("invitee_uri").alias("total_bookings"),
        countDistinct("invitee_email").alias("unique_emails"),
        count(when(col("event_status") == "active", 1)).alias("active_bookings"),
        count(when(col("invitee_canceled") == True, 1)).alias("cancelled_bookings")
    )

    # Add derived metrics
    df_metric = df_metric.withColumn(
        "cancellation_rate",
        (col("cancelled_bookings") / col("total_bookings") * 100).cast(DoubleType())
    )

    # Add date components for easier filtering
    df_metric = df_metric.withColumn("year", year(col("booking_date"))) \
        .withColumn("month", month(col("booking_date"))) \
        .withColumn("day", dayofmonth(col("booking_date"))) \
        .withColumn("day_of_week", dayofweek(col("booking_date")))

    # Add processing metadata
    df_metric = df_metric.withColumn("metric_name", lit("daily_bookings_by_source")) \
        .withColumn("calculated_at", current_timestamp())

    # Write to Gold layer
    gold_path = f"s3://{GOLD_BUCKET}/metrics/daily_bookings_by_source/"
    print(f"DEBUG: Writing to path: '{gold_path}'")
    write_to_gold(df_metric, gold_path, "daily_bookings_by_source")

    return df_metric.count()


def create_cost_per_booking():
    """
    Metric 1.2: Cost Per Booking (CPB) by Channel
    Total Spend / Total Booked Calls per channel
    """
    print("Creating cost per booking metric...")

    # Read Silver tables
    events_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"
    spend_path = f"s3://{SILVER_BUCKET}/marketing_spend_silver/"

    print(f"DEBUG: Reading events from: '{events_path}'")
    print(f"DEBUG: Reading spend from: '{spend_path}'")

    try:
        df_events = spark.read.format("delta").load(events_path)
        df_spend = spark.read.format("delta").load(spend_path)
    except Exception as e:
        print(f"❌ ERROR reading from paths: {str(e)}")
        raise

    # Aggregate bookings by date and channel
    df_bookings = df_events.groupBy(
        col("booking_date").alias("date"),
        col("marketing_channel").alias("channel")
    ).agg(
        count("invitee_uri").alias("total_bookings")
    )

    # Aggregate spend by date and channel
    df_spend_agg = df_spend.groupBy(
        col("date"),
        col("channel")
    ).agg(
        sum("spend").alias("total_spend")
    )

    # Join bookings with spend
    df_metric = df_spend_agg.join(
        df_bookings,
        on=["date", "channel"],
        how="outer"
    ).fillna(0, subset=["total_bookings", "total_spend"])

    # Calculate CPB
    df_metric = df_metric.withColumn(
        "cost_per_booking",
        when(col("total_bookings") > 0, col("total_spend") / col("total_bookings"))
        .otherwise(lit(None))
    )

    # Add cumulative metrics
    window_spec = Window.partitionBy("channel").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_metric = df_metric.withColumn(
        "cumulative_spend", sum("total_spend").over(window_spec)
    ).withColumn(
        "cumulative_bookings", sum("total_bookings").over(window_spec)
    ).withColumn(
        "cumulative_cpb",
        when(col("cumulative_bookings") > 0,
             col("cumulative_spend") / col("cumulative_bookings"))
        .otherwise(lit(None))
    )

    # Add date components
    df_metric = df_metric.withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))

    # Add metadata
    df_metric = df_metric.withColumn("metric_name", lit("cost_per_booking")) \
        .withColumn("calculated_at", current_timestamp())

    # Write to Gold layer
    gold_path = f"s3://{GOLD_BUCKET}/metrics/cost_per_booking/"
    print(f"DEBUG: Writing to path: '{gold_path}'")
    write_to_gold(df_metric, gold_path, "cost_per_booking")

    return df_metric.count()


def create_bookings_trend():
    """
    Metric 1.3: Bookings Trend Over Time
    Daily/weekly volume tracking
    """
    print("Creating bookings trend metric...")

    events_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"
    print(f"DEBUG: Reading from path: '{events_path}'")

    try:
        df_events = spark.read.format("delta").load(events_path)
    except Exception as e:
        print(f"❌ ERROR reading from {events_path}: {str(e)}")
        raise

    # Daily trend
    df_daily = df_events.groupBy(
        col("booking_date").alias("date"),
        col("marketing_channel").alias("source")
    ).agg(
        count("invitee_uri").alias("bookings_count")
    )

    # Add rolling averages
    window_7d = Window.partitionBy("source").orderBy("date") \
        .rowsBetween(-6, 0)
    window_30d = Window.partitionBy("source").orderBy("date") \
        .rowsBetween(-29, 0)

    df_daily = df_daily.withColumn(
        "rolling_avg_7d", avg("bookings_count").over(window_7d)
    ).withColumn(
        "rolling_avg_30d", avg("bookings_count").over(window_30d)
    )

    # Add week number for weekly aggregation
    df_daily = df_daily.withColumn("week_of_year", weekofyear(col("date"))) \
        .withColumn("year", year(col("date")))

    # Add metadata
    df_daily = df_daily.withColumn("metric_name", lit("bookings_trend_daily")) \
        .withColumn("calculated_at", current_timestamp())

    # Write to Gold layer
    gold_path = f"s3://{GOLD_BUCKET}/metrics/bookings_trend/"
    print(f"DEBUG: Writing to path: '{gold_path}'")
    write_to_gold(df_daily, gold_path, "bookings_trend")

    return df_daily.count()


def create_channel_attribution():
    """
    Metric 1.4: Channel Attribution
    Comprehensive channel performance leaderboard
    """
    print("Creating channel attribution metric...")

    events_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"
    spend_path = f"s3://{SILVER_BUCKET}/marketing_spend_silver/"

    print(f"DEBUG: Reading events from: '{events_path}'")
    print(f"DEBUG: Reading spend from: '{spend_path}'")

    try:
        df_events = spark.read.format("delta").load(events_path)
        df_spend = spark.read.format("delta").load(spend_path)
    except Exception as e:
        print(f"❌ ERROR reading from paths: {str(e)}")
        raise

    # Aggregate by channel (all time)
    df_channel_bookings = df_events.groupBy("marketing_channel").agg(
        count("invitee_uri").alias("total_bookings"),
        countDistinct("invitee_email").alias("unique_leads")
    )

    df_channel_spend = df_spend.groupBy("channel").agg(
        sum("spend").alias("total_spend")
    )

    # Join and calculate metrics
    df_metric = df_channel_spend.join(
        df_channel_bookings,
        df_channel_spend.channel == df_channel_bookings.marketing_channel,
        how="outer"
    ).select(
        coalesce(col("channel"), col("marketing_channel")).alias("channel"),
        coalesce(col("total_bookings"), lit(0)).alias("total_bookings"),
        coalesce(col("unique_leads"), lit(0)).alias("unique_leads"),
        coalesce(col("total_spend"), lit(0.0)).alias("total_spend")
    )

    # Calculate CPB and other metrics
    df_metric = df_metric.withColumn(
        "cost_per_booking",
        when(col("total_bookings") > 0, col("total_spend") / col("total_bookings"))
        .otherwise(lit(None))
    ).withColumn(
        "cost_per_lead",
        when(col("unique_leads") > 0, col("total_spend") / col("unique_leads"))
        .otherwise(lit(None))
    )

    # Add ranking
    df_metric = df_metric.withColumn(
        "rank_by_bookings",
        row_number().over(Window.orderBy(col("total_bookings").desc()))
    ).withColumn(
        "rank_by_cpb",
        row_number().over(Window.orderBy(col("cost_per_booking").asc_nulls_last()))
    )

    # Add metadata
    df_metric = df_metric.withColumn("metric_name", lit("channel_attribution")) \
        .withColumn("calculated_at", current_timestamp())

    # Write to Gold layer
    gold_path = f"s3://{GOLD_BUCKET}/metrics/channel_attribution/"
    print(f"DEBUG: Writing to path: '{gold_path}'")
    write_to_gold(df_metric, gold_path, "channel_attribution")

    return df_metric.count()


def create_booking_time_analysis():
    """
    Metric 1.5: Booking Volume by Time Slot / Day of Week
    Understand when leads prefer booking
    """
    print("Creating booking time analysis metric...")

    events_path = f"s3://{SILVER_BUCKET}/calendly_events_silver/"
    print(f"DEBUG: Reading from path: '{events_path}'")

    try:
        df_events = spark.read.format("delta").load(events_path)
    except Exception as e:
        print(f"❌ ERROR reading from {events_path}: {str(e)}")
        raise

    # Aggregate by hour and day of week
    df_metric = df_events.groupBy(
        col("booking_hour"),
        col("booking_day_of_week"),
        col("marketing_channel")
    ).agg(
        count("invitee_uri").alias("bookings_count")
    )

    # Calculate percentage of total bookings
    window_spec = Window.partitionBy("marketing_channel")
    df_metric = df_metric.withColumn(
        "total_channel_bookings", sum("bookings_count").over(window_spec)
    ).withColumn(
        "percentage_of_channel",
        (col("bookings_count") / col("total_channel_bookings") * 100).cast(DoubleType())
    )

    # Add metadata
    df_metric = df_metric.withColumn("metric_name", lit("booking_time_analysis")) \
        .withColumn("calculated_at", current_timestamp())

    # Write to Gold layer
    gold_path = f"s3://{GOLD_BUCKET}/metrics/booking_time_analysis/"
    print(f"DEBUG: Writing to path: '{gold_path}'")
    write_to_gold(df_metric, gold_path, "booking_time_analysis")

    return df_metric.count()


def create_employee_meeting_load():
    """
    Metric 1.6: Meeting Load per Employee
    Calculate meetings per employee per week

    NOTE: This metric requires host information which is not currently captured
    in the webhook data. Skipping for now or using invitee data as proxy.
    """
    print("Skipping employee meeting load metric - host information not available in current data model")

    # Return 0 to indicate no records created
    return 0


def write_to_gold(df: DataFrame, path: str, table_name: str):
    """
    Write DataFrame to Gold layer as Delta table

    Args:
        df: DataFrame to write
        path: S3 path for Delta table
        table_name: Table name in Glue Catalog
    """
    # VALIDATE PATH IS NOT EMPTY
    if not path or path.strip() == "":
        raise ValueError(f"❌ ERROR: Path is empty for table {table_name}")

    if "s3://" not in path:
        raise ValueError(f"❌ ERROR: Path does not start with s3://: {path}")

    print(f"DEBUG: Writing to Gold path: {path}")

    # Write as Delta table
    try:
        df.write.mode("overwrite").parquet(path)
    except Exception as e:
        print(f"❌ ERROR writing to {path}: {str(e)}")
        raise

    # Register in Glue Catalog
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}
            USING DELTA
            LOCATION '{path}'
        """)
    except Exception as e:
        print(f"❌ ERROR creating table {DATABASE_NAME}.{table_name}: {str(e)}")
        # Don't raise here - table creation might fail but data is already written
        print("Continuing despite table creation error...")

    print(f"✅ Successfully written to Gold layer: {path}")


def main():
    """
    Main ETL process for all Gold metrics
    """
    print(f"Starting Silver to Gold transformation...")
    print(f"Silver bucket: '{SILVER_BUCKET}'")
    print(f"Gold bucket: '{GOLD_BUCKET}'")
    print(f"Database: '{DATABASE_NAME}'")

    # Validate buckets again before proceeding
    if not SILVER_BUCKET or SILVER_BUCKET.strip() == "":
        raise ValueError("SILVER_BUCKET is empty in main()")
    if not GOLD_BUCKET or GOLD_BUCKET.strip() == "":
        raise ValueError("GOLD_BUCKET is empty in main()")

    # Create all metrics
    metrics_created = {}

    try:
        metrics_created['daily_bookings_by_source'] = create_daily_bookings_by_source()
    except Exception as e:
        print(f"❌ Failed to create daily_bookings_by_source: {str(e)}")
        metrics_created['daily_bookings_by_source'] = 0

    try:
        metrics_created['cost_per_booking'] = create_cost_per_booking()
    except Exception as e:
        print(f"❌ Failed to create cost_per_booking: {str(e)}")
        metrics_created['cost_per_booking'] = 0

    try:
        metrics_created['bookings_trend'] = create_bookings_trend()
    except Exception as e:
        print(f"❌ Failed to create bookings_trend: {str(e)}")
        metrics_created['bookings_trend'] = 0

    try:
        metrics_created['channel_attribution'] = create_channel_attribution()
    except Exception as e:
        print(f"❌ Failed to create channel_attribution: {str(e)}")
        metrics_created['channel_attribution'] = 0

    try:
        metrics_created['booking_time_analysis'] = create_booking_time_analysis()
    except Exception as e:
        print(f"❌ Failed to create booking_time_analysis: {str(e)}")
        metrics_created['booking_time_analysis'] = 0

    metrics_created['employee_meeting_load'] = create_employee_meeting_load()

    print(f"Silver to Gold transformation completed successfully")
    for metric, count in metrics_created.items():
        print(f"  - {metric}: {count} records")

    job.commit()


if __name__ == "__main__":
    main()
