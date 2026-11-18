CREATE OR REFRESH MATERIALIZED VIEW
silver_bls_observations_expected
(
    CONSTRAINT valid_series_id
        EXPECT (series_id IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_year
        EXPECT (year IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_period
        EXPECT (period IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_value
        EXPECT (value IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_quarter
        EXPECT (
            period IN ('Q01', 'Q02', 'Q03', 'Q04')
        )
        ON VIOLATION DROP ROW
)
COMMENT
'BLS observations with formal Lakeflow expectations'
AS
SELECT
    series_id,
    year,
    period,
    value,
    footnote_codes,
    _source_file,
    _source_file_name,
    _source_modified_at
FROM silver_bls_observations;
CREATE OR REFRESH MATERIALIZED VIEW
silver_us_population_expected
(
    CONSTRAINT population_year_not_null
        EXPECT (year IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT population_not_null
        EXPECT (population IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT positive_population
        EXPECT (population > 0)
        ON VIOLATION DROP ROW
)
COMMENT
'US population with formal Lakeflow expectations'
AS
SELECT
    year,
    nation,
    population
FROM silver_us_population;