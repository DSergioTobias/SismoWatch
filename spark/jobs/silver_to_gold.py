"""Spark job: silver → gold (agregados para consumo).

Lee `silver/events` (parquet particionado por hora) y calcula:
  - actividad por hora: conteo, magnitud máxima/promedio, alertas tsunami
  - top de regiones por actividad
Escribe parquet en `gold/hourly_activity/` y `gold/region_activity/`
(overwrite completo: la tabla es pequeña y se recalcula por completo).
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sismowatch_silver_to_gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="s3a://silver")
    parser.add_argument("--gold", default="s3a://gold")
    parser.add_argument("--days", type=int, default=7,
                        help="ventana de agregación en días")
    args = parser.parse_args()

    spark = build_spark()

    events = spark.read.parquet(f"{args.silver.rstrip('/')}/events")
    events = events.filter(
        F.col("event_time") >= F.expr(f"current_timestamp() - interval {args.days} days")
    )

    hourly = (
        events.withColumn("bucket_hour", F.date_trunc("hour", F.col("event_time")))
        .groupBy("bucket_hour")
        .agg(
            F.count("*").cast("int").alias("event_count"),
            F.max("mag").alias("max_mag"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.sum(F.when(F.col("tsunami"), 1).otherwise(0))
             .cast("int").alias("tsunami_count"),
        )
        .orderBy("bucket_hour")
    )

    regions = (
        events.groupBy("region")
        .agg(
            F.count("*").cast("int").alias("event_count"),
            F.max("mag").alias("max_mag"),
        )
        .orderBy(F.desc("event_count"))
        .limit(50)
    )

    hourly.coalesce(1).write.mode("overwrite").parquet(
        f"{args.gold.rstrip('/')}/hourly_activity")
    regions.coalesce(1).write.mode("overwrite").parquet(
        f"{args.gold.rstrip('/')}/region_activity")

    print(f"[silver_to_gold] hourly={hourly.count()} regiones={regions.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
