from pyspark import pipelines as dp


# ============================================================
# Q1 - SPARK SQL
# ============================================================

@dp.materialized_view(
    name="gold_q1_population_statistics_sql",
    comment="Q1 Spark SQL alternative"
)
def gold_q1_population_statistics_sql():

    return spark.sql("""
        SELECT
            COUNT(*) AS number_of_years,

            AVG(population)
                AS mean_population,

            STDDEV_SAMP(population)
                AS sample_standard_deviation,

            STDDEV_POP(population)
                AS population_standard_deviation,

            MIN(year) AS from_year,

            MAX(year) AS to_year

        FROM silver_us_population

        WHERE year BETWEEN 2013 AND 2018
    """)


# ============================================================
# Q2 - SPARK SQL
# ============================================================

@dp.materialized_view(
    name="gold_q2_bls_best_year_sql",
    comment="Q2 Spark SQL alternative"
)
def gold_q2_bls_best_year_sql():

    return spark.sql("""
        WITH annual_totals AS
        (
            SELECT
                series_id,
                year,

                SUM(value)
                    AS annual_value_sum,

                COUNT(DISTINCT period)
                    AS quarter_count

            FROM silver_bls_observations

            GROUP BY
                series_id,
                year
        ),

        ranked AS
        (
            SELECT
                series_id,
                year,
                annual_value_sum,
                quarter_count,

                ROW_NUMBER() OVER
                (
                    PARTITION BY series_id

                    ORDER BY
                        annual_value_sum DESC,
                        year DESC
                ) AS year_rank

            FROM annual_totals
        )

        SELECT
            ranked.series_id,
            metadata.series_title,
            metadata.sector_code,
            metadata.measure_code,
            metadata.duration_code,
            metadata.seasonal,

            ranked.year
                AS best_year,

            ranked.annual_value_sum,
            ranked.quarter_count

        FROM ranked

        LEFT JOIN silver_bls_series metadata

            ON ranked.series_id =
               metadata.series_id

        WHERE ranked.year_rank = 1
    """)


# ============================================================
# Q3 - SPARK SQL
# ============================================================

@dp.materialized_view(
    name="gold_q3_prs30006032_population_sql",
    comment="Q3 Spark SQL alternative"
)
def gold_q3_prs30006032_population_sql():

    return spark.sql("""
        SELECT
            bls.series_id,
            bls.year,
            bls.period,
            bls.value,
            pop.population,

            pop.population IS NOT NULL
                AS population_available

        FROM silver_bls_observations bls

        LEFT JOIN silver_us_population pop

            ON bls.year = pop.year

        WHERE
            bls.series_id = 'PRS30006032'

            AND bls.period = 'Q01'
    """)