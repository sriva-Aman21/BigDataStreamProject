import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# 1. Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("SupplyChainStreamProcessing")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Event Schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("delivery_time", IntegerType(), True),
])

# 3. Read Stream from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "supply_chain_events")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse & Validate JSON
events = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("event"))
    .select("event.*")
    .filter(
        col("transaction_id").isNotNull()
        & col("product").isNotNull()
        & col("quantity").isNotNull()
    )
)

# 5. Micro-Batch Sink to PostgreSQL (Analytics + Anomalies)
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # A. Aggregate Product Summaries
    summary = (
        batch_df.groupBy("product")
        .agg(
            count("*").alias("transaction_count"),
            sum("quantity").alias("total_quantity"),
        )
    )
    product_rows = summary.collect()

    # B. Filter Anomalous Events
    anomalies_df = (
        batch_df.filter(
            (col("unit_price") > 400.0) | (col("delivery_time") > 10)
        )
        .withColumn(
            "anomaly_reason",
            when((col("unit_price") > 400.0) & (col("delivery_time") > 10), "High Price & Extreme Delay")
            .when(col("unit_price") > 400.0, "High Unit Price (> 400)")
            .otherwise("Severe Delivery Delay (> 10 days)")
        )
    )
    anomaly_rows = anomalies_df.collect()

    try:
        connection = psycopg2.connect(
            host=os.environ.get("DB_HOST", "host.docker.internal"),
            port=int(os.environ.get("DB_PORT", "5432")),
            dbname=os.environ.get("DB_NAME", "supply_chain_db"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", ""),
            connect_timeout=10,
        )

        with connection:
            with connection.cursor() as cursor:
                # 1. Update Product Analytics
                for row in product_rows:
                    cursor.execute("""
                        INSERT INTO product_analytics (product, transaction_count, total_quantity, last_updated)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (product) DO UPDATE SET
                            transaction_count = product_analytics.transaction_count + EXCLUDED.transaction_count,
                            total_quantity = product_analytics.total_quantity + EXCLUDED.total_quantity,
                            last_updated = CURRENT_TIMESTAMP
                    """, (row["product"], row["transaction_count"], row["total_quantity"]))

                # 2. Insert Detected Anomalies
                for anom in anomaly_rows:
                    cursor.execute("""
                        INSERT INTO supply_chain_anomalies
                            (transaction_id, timestamp, product, region, quantity, unit_price, delivery_time, anomaly_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                    """, (
                        anom["transaction_id"],
                        anom["timestamp"],
                        anom["product"],
                        anom["region"],
                        anom["quantity"],
                        anom["unit_price"],
                        anom["delivery_time"],
                        anom["anomaly_reason"],
                    ))

        connection.close()
        print(f"Batch {batch_id} saved: {len(product_rows)} products, {len(anomaly_rows)} anomalies flagged.")

    except Exception as err:
        print(f"Error saving batch {batch_id}: {err}")

# 6. Start Streaming Query
checkpoint_dir = "/opt/spark/work-dir/checkpoints/supply_chain"

query = (
    events.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint_dir)
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark is listening to Kafka and detecting anomalies in real time.")
query.awaitTermination()