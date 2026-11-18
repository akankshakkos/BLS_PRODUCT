from pyspark import pipelines as dp
from pyspark.sql import functions as F


VOLUME_ROOT = (
    "/Volumes/workspace/"
    "bls_productivity/raw_files"
)

BLS_RAW_PATH = (
    f"{VOLUME_ROOT}/current/bls"
)

POPULATION_RAW_PATH = (
    f"{VOLUME_ROOT}/current/population/population.json"
)


def read_bls_tsv(path):

    source_df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("mode", "PERMISSIVE")
        .csv(path)
        .select(
            "*",
            F.col("_metadata.file_path").alias(
                "_source_file"
            ),
            F.col("_metadata.file_name").alias(
                "_source_file_name"
            ),
            F.col(
                "_metadata.file_modification_time"
            ).alias(
                "_source_modified_at"
            )
        )
    )

    metadata_columns = {
        "_source_file",
        "_source_file_name",
        "_source_modified_at"
    }

    cleaned_columns = []

    for source_column in source_df.columns:

        if source_column in metadata_columns:
            continue

        clean_name = (
            source_column
            .strip()
            .lower()
            .replace(" ", "_")
        )

        cleaned_columns.append(
            F.trim(
                F.col(f"`{source_column}`")
            ).alias(clean_name)
        )

    return source_df.select(
        *cleaned_columns,
        "_source_file",
        "_source_file_name",
        "_source_modified_at"
    )


# ============================================================
# BRONZE: BLS OBSERVATIONS
# ============================================================

@dp.materialized_view(
    name="bronze_bls_observations",
    comment="Raw BLS productivity observations"
)
def bronze_bls_observations():

    raw_df = read_bls_tsv(
        f"{BLS_RAW_PATH}/pr.data.*"
    )

    return raw_df.select(
        "series_id",
        "year",
        "period",
        "value",
        "footnote_codes",
        "_source_file",
        "_source_file_name",
        "_source_modified_at",
        F.current_timestamp().alias(
            "_bronze_loaded_at"
        )
    )


# ============================================================
# BRONZE: BLS SERIES METADATA
# ============================================================

@dp.materialized_view(
    name="bronze_bls_series",
    comment="Raw BLS series metadata"
)
def bronze_bls_series():

    raw_df = read_bls_tsv(
        f"{BLS_RAW_PATH}/pr.series"
    )

    return raw_df.select(
        "series_id",
        "sector_code",
        "class_code",
        "measure_code",
        "duration_code",
        "seasonal",
        "base_year",
        "footnote_codes",
        "series_title",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
        "_source_file",
        "_source_file_name",
        "_source_modified_at",
        F.current_timestamp().alias(
            "_bronze_loaded_at"
        )
    )


# ============================================================
# BRONZE: POPULATION
# ============================================================

@dp.materialized_view(
    name="bronze_population",
    comment="Raw Data USA population records"
)
def bronze_population():

    raw = (
        spark.read
        .option("multiline", "true")
        .json(POPULATION_RAW_PATH)
    )

    return (
        raw
        .select(
            F.explode("data").alias("record")
        )
        .select(
            F.col("record.Year").alias(
                "Year"
            ),
            F.col("record.Nation").alias(
                "Nation"
            ),
            F.col("record.Population").alias(
                "Population"
            ),
            F.current_timestamp().alias(
                "_bronze_loaded_at"
            )
        )
    )