from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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