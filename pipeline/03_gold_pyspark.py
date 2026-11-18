from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# QUESTION 1
#
# Mean and standard deviation of annual US population,
# 2013 through 2018 inclusive.
# ============================================================

@dp.materialized_view(
    name="gold_q1_population_statistics",
    comment="Q1 primary PySpark solution"
)
def gold_q1_population_statistics():

    population = (
        spark.read.table(
            "silver_us_population"
        )
        .filter(
            F.col("year").between(
                2013,
                2018
            )
        )
    )

    return population.agg(
        F.count("*").alias(
            "number_of_years"
        ),

        F.avg("population").alias(
            "mean_population"
        ),

        F.stddev_samp(
            "population"
        ).alias(
            "sample_standard_deviation"
        ),

        F.stddev_pop(
            "population"
        ).alias(
            "population_standard_deviation"
        ),

        F.min("year").alias(
            "from_year"
        ),

        F.max("year").alias(
            "to_year"
        )
    )


# ============================================================
# QUESTION 2
#
# Best year for every series_id based on
# largest sum of quarterly value.
# ============================================================

@dp.materialized_view(
    name="gold_q2_bls_best_year",
    comment="Q2 primary PySpark solution"
)
def gold_q2_bls_best_year():

    observations = spark.read.table(
        "silver_bls_observations"
    )

    metadata = (
        spark.read.table(
            "silver_bls_series"
        )
        .select(
            "series_id",
            "series_title",
            "sector_code",
            "measure_code",
            "duration_code",
            "seasonal"
        )
    )

    annual = (
        observations
        .groupBy(
            "series_id",
            "year"
        )
        .agg(
            F.sum("value").alias(
                "annual_value_sum"
            ),
            F.countDistinct(
                "period"
            ).alias(
                "quarter_count"
            )
        )
    )

    ranking = (
        Window
        .partitionBy("series_id")
        .orderBy(
            F.col(
                "annual_value_sum"
            ).desc(),
            F.col("year").desc()
        )
    )

    best = (
        annual
        .withColumn(
            "_rank",
            F.row_number().over(
                ranking
            )
        )
        .filter(
            F.col("_rank") == 1
        )
        .drop("_rank")
    )

    return (
        best
        .join(
            metadata,
            on="series_id",
            how="left"
        )
        .select(
            "series_id",
            "series_title",
            "sector_code",
            "measure_code",
            "duration_code",
            "seasonal",

            F.col("year").alias(
                "best_year"
            ),

            "annual_value_sum",
            "quarter_count"
        )
    )


# ============================================================
# QUESTION 3
#
# PRS30006032 / Q01 by year,
# joined with US population where available.
# ============================================================

@dp.materialized_view(
    name="gold_q3_prs30006032_population",
    comment="Q3 primary PySpark solution"
)
def gold_q3_prs30006032_population():

    bls = (
        spark.read.table(
            "silver_bls_observations"
        )
        .filter(
            (
                F.col("series_id")
                == "PRS30006032"
            )
            &
            (
                F.col("period")
                == "Q01"
            )
        )
        .select(
            "series_id",
            "year",
            "period",
            "value"
        )
    )

    population = (
        spark.read.table(
            "silver_us_population"
        )
        .select(
            "year",
            "population"
        )
    )

    return (
        bls
        .join(
            population,
            on="year",
            how="left"
        )
        .select(
            "series_id",
            "year",
            "period",
            "value",
            "population",

            F.col(
                "population"
            )
            .isNotNull()
            .alias(
                "population_available"
            )
        )
    )