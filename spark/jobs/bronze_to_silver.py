"""Spark job: bronze → silver.

Lee los JSON crudos de MinIO (bucket `bronze`, particionados por día),
limpia, tipa y sanea cada evento, y escribe parquet particionado por hora
en `silver/events/` con overwrite dinámico (idempotente por partición).

Las mismas reglas de transformación que la versión Python
(`include/transform`) pero ejecutadas de forma distribuida por Spark.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CONTROL_CHARS = r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f<>]"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sismowatch_bronze_to_silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Lee todos los .json de bronze.

    Cada objeto JSON de bronze es un ARRAY de eventos planos (escrito por
    el DAG de ingesta); spark.read.json devuelve una fila por elemento.
    """
    df = spark.read.json(f"{bronze_path.rstrip('/')}/**/*.json")
    return df


def sanitize_text(col) -> "F.Column":
    """Equivalente Spark de include/transform.sanitize_text."""
    cleaned = F.regexp_replace(F.col(col).cast("string"), CONTROL_CHARS, " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    return F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(
        F.substring(cleaned, 1, 300))


def transform(df: DataFrame) -> DataFrame:
    """Limpieza + tipado, replicando event_to_staging en PySpark."""
    # magnitud → double (los feeds traen string o number)
    mag = F.col("mag").cast(T.DoubleType())

    # coordenadas
    lon = F.col("longitude").cast(T.DoubleType())
    lat = F.col("latitude").cast(T.DoubleType())
    depth = F.col("depth_km").cast(T.DoubleType())

    # fechas: milisegundos epoch → timestamp UTC
    event_ts = F.from_unixtime(F.col("event_time").cast(T.LongType()) / 1000.0)
    updated_ts = F.when(F.col("updated_time").isNotNull(),
                        F.from_unixtime(F.col("updated_time").cast(T.LongType()) / 1000.0))

    # tsunami → booleano (0/1, true/false, 'true'...)
    tsunami = (
        F.when(F.col("tsunami").cast("string").isin("1", "true", "True", "t"), True)
        .when(F.col("tsunami").cast("string").isin("0", "false", "False", "f", ""), False)
        .otherwise(None)
    )

    place = sanitize_text("place")
    region_raw = F.when(
        F.instr(place, ",") > 0,
        F.trim(F.substring(place, F.instr(place, ",") + 1, 200)),
    ).otherwise(place)
    region = F.when(F.length(region_raw) == 0, F.lit("unknown")).otherwise(region_raw)

    return (
        df.select(
            F.col("source_id"),
            F.col("event_uid").cast("string").alias("event_uid"),
            mag.alias("mag"),
            place.alias("place"),
            region.alias("region"),
            event_ts.alias("event_time"),
            updated_ts.alias("updated_time"),
            tsunami.alias("tsunami"),
            F.col("sig").cast(T.IntegerType()).alias("sig"),
            sanitize_text("status").alias("status"),
            sanitize_text("mag_type").alias("mag_type"),
            sanitize_text("event_type").alias("event_type"),
            sanitize_text("url").alias("url"),
            lon.alias("longitude"),
            lat.alias("latitude"),
            depth.alias("depth_km"),
        )
        .withColumn("event_hour",
                    F.date_format(F.col("event_time"), "yyyy-MM-dd-HH"))
        .dropna(subset=["source_id", "event_uid", "event_time"])
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default="s3a://bronze")
    parser.add_argument("--silver", default="s3a://silver")
    args = parser.parse_args()

    spark = build_spark()
    raw = read_bronze(spark, args.bronze)
    silver = transform(raw)

    silver.write.partitionBy("event_hour").mode("overwrite").parquet(
        f"{args.silver.rstrip('/')}/events")
    count = silver.count()
    print(f"[bronze_to_silver] {count} filas → {args.silver}/events")
    spark.stop()


if __name__ == "__main__":
    main()
