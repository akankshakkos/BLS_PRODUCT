PROCESS
1. Overview

I implemented this solution as an end-to-end Databricks data engineering pipeline using two external data sources:

BLS Productivity Time Series
Data USA annual US population API

I separated the solution into two main stages:

Source acquisition and raw data landing
Declarative Bronze, Silver, and Gold transformations

The overall flow is:

BLS Productivity Directory + Data USA Population API
 → Dynamic Raw Ingestion
 → Unity Catalog Volume
 → Bronze
 → Silver + Data Quality
 → Gold
 → PySpark / Spark SQL Validation

2. Architecture
2.1 Separation of ingestion and transformation

I intentionally separated source acquisition from the Lakeflow transformation pipeline.

The raw ingestion layer is responsible for:

HTTP communication
Dynamic BLS file discovery
Downloading files
SHA-256 change detection
Safe file writes
Raw data landing in a Unity Catalog Volume

The Lakeflow Spark Declarative Pipeline is responsible for:

Bronze ingestion
Silver cleansing and validation
Duplicate resolution
Data-quality expectations
Gold analytical outputs

I chose this separation because HTTP calls and file writes are side-effect-oriented operations, while the transformation pipeline should remain deterministic and declarative.

3. Raw Data Landing

I used a Unity Catalog Volume as the raw landing area.

The logical structure is:

raw_files/

current/
bls/
population/
archive/
bls/
population/

The current area represents the latest upstream source state consumed by the pipeline.

The raw landing layer preserves source data independently from downstream transformations, improving auditability, reproducibility, and troubleshooting.

4. Dynamic BLS File Discovery

The BLS source is a directory containing multiple productivity data and metadata files.

I deliberately did not hardcode the list of BLS filenames.

The ingestion process:

Requests the BLS productivity directory.
Parses the HTML directory listing.
Extracts file links.
Removes navigation and directory links.
Restricts discovery to the expected BLS host and directory.
Downloads the discovered files.

This means that if BLS adds another source file, ingestion can discover it without requiring a code change.

5. BLS HTTP Access

The BLS server can reject generic automated HTTP requests.

I therefore use an identifiable application User-Agent rather than impersonating a browser.

The contact address is treated as configuration and should not be committed as sensitive information in the repository.

6. Safe and Repeatable Ingestion

A major requirement was ensuring that ingestion can be safely rerun.

I use SHA-256 hashing to determine whether source content is new, changed, or unchanged.

For each source file:

Download the source bytes.
Calculate the SHA-256 digest.
Compare that digest with the currently stored file.
Determine whether the file needs to be rewritten.

Behavior:

New file: write it.
Same filename and same hash: mark unchanged and skip rewriting.
Same filename and different hash: treat as changed and replace the current version.
Previously available file no longer present upstream: remove the stale file from the current snapshot.

This makes ingestion idempotent for unchanged source data.

7. Why SHA-256?

A filename identifies a logical object but does not prove its contents are unchanged.

For example:

Day 1:

pr.series
 SHA-256 = ABC

Day 2:

pr.series
 SHA-256 = XYZ

The filename is identical, but the contents changed.

SHA-256 gives the ingestion process a deterministic content fingerprint.

I preferred content hashing to relying only on timestamps because the relevant question is whether the actual source bytes changed.

8. Atomic File Writes

Files are initially written to a temporary uniquely named file.

Only after the complete write succeeds is the temporary file moved into the final location.

The pattern is:

Download
 → Temporary file
 → Complete successful write
 → Atomic replace
 → Current source file

This reduces the risk that downstream processing reads a partially written file after an interrupted ingestion.

9. Population API

The Data USA population API is processed separately from BLS.

The complete API response is saved as JSON in the Unity Catalog Volume before transformation.

The design is:

Data USA API
 → Raw JSON
 → Bronze
 → Silver
 → Gold

This preserves the original API payload for reproducibility and troubleshooting.

10. Medallion Architecture

I modeled the transformation pipeline using Bronze, Silver, and Gold layers.

Bronze answers:

What did the source provide?

Silver answers:

What data can downstream consumers trust?

Gold answers:

What analytical question am I answering?

11. Bronze Layer

Bronze contains:

bronze_bls_observations
bronze_bls_series
bronze_population

The BLS Bronze tables also retain source lineage:

source file path
source filename
source modification timestamp
Bronze load timestamp

I intentionally keep Bronze relatively close to the original source.

12. Unity Catalog File Metadata

Source lineage is captured using:

_metadata.file_path
_metadata.file_name
_metadata.file_modification_time

These fields are selected immediately after reading the source file and propagated into Bronze.

This lineage also supports deterministic duplicate resolution in Silver.

13. Silver Layer

Silver performs:

Datatype conversion
Whitespace normalization
Non-null key validation
Numeric-value validation
Quarter validation
Year validation
Deterministic duplicate resolution
Population validation
Rejected-record handling

The BLS observation business key is:

series_id + year + period

The population business grain is:

nation + year

14. Quarterly Filtering

Question 2 specifically asks for a sum across quarters.

Therefore Silver retains only:

Q01
Q02
Q03
Q04

Other BLS period types are excluded.

This prevents annual or other source period values from being added to quarterly observations and distorting annual totals.

15. Overlapping BLS Files

BLS provides both:

pr.data.0.Current
pr.data.1.AllData

These files can contain overlapping logical observations.

If both copies entered Gold calculations, annual totals could be incorrectly doubled.

Bronze preserves the source evidence, while Silver resolves the overlap.

16. Deterministic Deduplication

Silver deduplicates using:

series_id + year + period

Source precedence is:

pr.data.0.Current
pr.data.1.AllData
Other matching files

A Spark Window partitions records by the business key and orders candidates according to this precedence.

ROW_NUMBER() then retains one trusted record.

I preferred this over a simple dropDuplicates() because the winning row is explicit and reproducible.

17. Human-Readable BLS Metadata

The BLS pr.series file contains series_title.

Question 2 therefore returns both:

series_id
series_title

This lets someone unfamiliar with BLS codes understand what each series represents.

18. Data Quality

I implemented two complementary data-quality mechanisms:

Rejected/quarantine datasets
Formal Lakeflow expectations
19. Rejected Records

BLS records failing validation are preserved with rejection reasons.

Examples include:

NULL_SERIES_ID
INVALID_YEAR
NULL_PERIOD
INVALID_VALUE
NOT_QUARTERLY_PERIOD

I preferred retaining invalid records separately rather than silently dropping them.

This improves auditability and troubleshooting.

20. Lakeflow Expectations

Formal Lakeflow expectations enforce quality rules such as:

Series ID must not be null
Year must not be null
Period must not be null
Value must not be null
Period must represent a valid quarter
Population must not be null
Population must be positive

Using expectations together with rejected datasets provides both data-quality metrics and access to problematic records.

21. Gold Layer

Gold contains business-facing analytical outputs.

The calculations are implemented inside the pipeline rather than only in the validation notebook.

The validation notebook verifies Gold results.

22. Question 1

Question 1 asks for the mean and standard deviation of annual US population from 2013 through 2018 inclusive.

The expected years are:

2013
 2014
 2015
 2016
 2017
 2018

Therefore six annual observations should participate.

The Gold dataset includes:

number_of_years
mean_population
sample_standard_deviation
population_standard_deviation
from_year
to_year

The count and date boundaries provide useful sanity checks.

23. Standard Deviation

The requirement does not explicitly specify sample versus population standard deviation.

I therefore calculate both:

Sample standard deviation
Population standard deviation

This makes the statistical interpretation explicit.

24. Question 2

Question 2 asks for the best year for every BLS series.

The best year is defined as the year with the largest sum of quarterly values.

The first calculation groups by:

series_id + year

and calculates:

SUM(value)

I also calculate quarter_count using distinct period values.

This makes it visible how many quarters contributed to each annual total.

25. Q2 Ranking

Years are ranked within each series using:

annual_value_sum DESC

followed by:

year DESC

ROW_NUMBER() is used to select rank 1.

I chose ROW_NUMBER() instead of RANK() because the requirement expects one best year per series.

If two years have the same annual sum, the latest year wins as a deterministic tie-break rule.

26. Question 3

Question 3 filters BLS observations to:

series_id = PRS30006032

and:

period = Q01

The resulting observations are joined to annual US population using year.

27. Why LEFT JOIN for Question 3?

I used a LEFT JOIN from BLS to population.

The requirement says population should be included where available.

Therefore missing population must not remove an otherwise valid BLS observation.

With the LEFT JOIN:

BLS observations remain available for requested years.
Population is populated where a matching year exists.
Population is NULL where unavailable.

The Gold output also includes population_available.

28. PySpark and Spark SQL

Every analytical question is implemented twice:

PySpark
Spark SQL

I chose PySpark as the primary implementation because the transformation pipeline is Python-oriented and DataFrame transformations integrate naturally with the surrounding engineering logic.

Spark SQL remains an independently executable alternative.

29. Gold Outputs

Question 1:

gold_q1_population_statistics
gold_q1_population_statistics_sql

Question 2:

gold_q2_bls_best_year
gold_q2_bls_best_year_sql

Question 3:

gold_q3_prs30006032_population
gold_q3_prs30006032_population_sql
30. SQL and PySpark Reconciliation

I explicitly validate that SQL and PySpark produce equivalent outputs.

For Question 1, I compare numerical differences between equivalent aggregates.

The expected difference is zero or negligible floating-point precision.

For Questions 2 and 3, I use EXCEPT comparisons.

31. Why Bidirectional EXCEPT?

For Question 2 I validate both:

PySpark EXCEPT SQL

and:

SQL EXCEPT PySpark

This matters because one-direction comparison alone may miss extra rows on the other side.

Both comparisons should return zero rows.

32. Pipeline File Structure

The final Lakeflow pipeline is separated into:

01_bronze.py
02_silver.py
03_gold_pyspark.py
04_gold_sql.py
05_expectations.sql

I preferred multiple focused files because this improves:

Readability
Debugging
Code review
Maintainability
Separation of responsibilities
33. Validation

A separate validation notebook checks:

Pipeline tables exist
Question 1 output
Question 1 SQL/PySpark agreement
Question 2 output
Question 2 bidirectional reconciliation
Question 3 output
Question 3 SQL/PySpark reconciliation
Silver quarter domain
Rejected-record behavior

The validation notebook verifies the pipeline rather than implementing the analytical logic.

Trade-offs
34. Schema Drift

The current solution assumes key source fields remain available.

For a production client I would add explicit schema-contract validation.

I would distinguish:

Additive change: potentially compatible.

Removed/renamed key column: breaking change.

Breaking schema drift should generate an alert rather than silently modifying Gold output.

35. Data Volume

The current BLS dataset is modest.

At significantly larger scale I would evaluate:

Incremental processing
Partitioning
Delta clustering
Shuffle reduction
Incremental state
Query-plan optimization

groupBy and Window operations require Spark shuffles and become increasingly important as data grows.

36. Cost

In production I would monitor:

Pipeline execution time
Serverless compute usage
Source data volume
File download volume
Refresh frequency
Shuffle-heavy transformations
Unnecessary full-snapshot processing

Refresh frequency should align with source update frequency and business freshness requirements.

37. Access Control

For a real client I would separate Unity Catalog permissions by layer.

For example:

Raw Volume: engineering/service identities

Bronze: engineering

Silver: trusted engineering and analytical users

Gold: broader analytical consumers

Rejected records may need tighter access.

38. Secrets and Configuration

Environment-specific configuration should be externalized.

Examples include:

Catalog
Schema
Volume
Endpoint URLs
Contact address
Credentials for future authenticated sources

Sensitive values should use approved secret-management mechanisms.

Passwords, tokens, OAuth secrets and Databricks personal access tokens should never be committed to Git.

39. Monitoring

For production I would monitor:

Ingestion
Discovered files
New files
Changed files
Unchanged files
Removed files
HTTP failures
Bytes downloaded
Ingestion duration
Data Quality
Rejected rows
Rejection reasons
Expectation results
Unexpected periods
Missing key fields
Pipeline
Update success/failure
Execution duration
Source freshness
Table row-count changes
Gold output counts
40. Production Ingestion State

For a larger implementation I would introduce a Delta control table.

Potential fields:

source_name
source_url
filename
sha256
source_modified_at
ingested_at
run_id
status

This would give transactional and SQL-queryable ingestion state.

41. Orchestration

For production I would orchestrate:

Raw Ingestion
 → Lakeflow Pipeline
 → Validation / Monitoring

using Databricks Workflows.

The transformation pipeline should run only after raw landing succeeds.

Retrospective
42. BLS HTTP Access

One challenging part was automated access to the BLS download server.

Using an identifiable application User-Agent provided a transparent automated-access approach.

This reinforced that public data sources can still have operational access requirements.

43. Dynamic Directory Processing

The requirement to ingest the complete BLS directory was more complex than downloading one known file.

The ingestion process had to distinguish:

Actual source files
Parent-directory navigation
Directories
Unrelated paths

while remaining dynamic enough to discover future files.

44. Unity Catalog Metadata

A technical challenge was preserving source filenames in the Unity Catalog environment.

The working solution used:

_metadata.file_path
_metadata.file_name
_metadata.file_modification_time

This improved lineage and helped deterministic duplicate resolution.

45. Overlapping BLS Files

The largest analytical correctness risk was overlap between Current and AllData.

Without explicit resolution, quarterly observations could be counted multiple times and annual totals could be incorrect.

The final architecture preserves both sources in Bronze but resolves the logical observation deterministically in Silver.

46. Lakeflow Execution Model

Another important lesson was the distinction between:

Running a normal Databricks notebook
Running a Lakeflow declarative pipeline

Pipeline functions declare datasets.

Lakeflow builds the dependency graph and materializes those datasets.

Running notebook cells alone is therefore not equivalent to running the pipeline.

47. Data Quality Expectations

The initial interactive Python environment did not expose the expectation behavior required by the final implementation.

I first implemented explicit Silver validation and rejected-record datasets.

Formal pipeline expectations were then added using pipeline SQL.

The final solution therefore contains:

Explicit rejection/quarantine handling
Lakeflow-integrated expectations
48. What Was Hardest?

The analytical SQL itself was not the hardest part.

The most difficult areas were:

Dynamic BLS directory discovery
Safe rerunnable ingestion
Handling BLS HTTP behavior
Unity Catalog file lineage
Resolving overlapping BLS files
Understanding notebook versus Lakeflow execution
Adding formal data-quality expectations
Proving SQL and PySpark equivalence
49. What I Would Improve Next

Given more time for a production implementation, I would:

Add a Delta ingestion-control table.
Persist complete historical source versions.
Add explicit source-schema contracts.
Parameterize catalog, schema, Volume and source configuration.
Add source-freshness alerts.
Add unit tests for discovery, hashing, deduplication and ranking.
Add CI/CD across dev, test and production.
Orchestrate ingestion and Lakeflow with Databricks Workflows.
Add monitoring dashboards for ingestion and data quality.
Document operational recovery and backfill procedures.
50. Final Result

The completed solution provides:

Dynamic BLS source discovery
Complete raw landing in a Unity Catalog Volume
Data USA population JSON landing
SHA-256-based rerun/change detection
Bronze/Silver/Gold modeling
Source-file lineage
Deterministic duplicate resolution
Data-quality filtering
Rejected-record handling
Formal Lakeflow expectations
PySpark implementations
Spark SQL alternatives
Readable BLS series labels
SQL/PySpark reconciliation
Repeatable validation

The resulting pipeline can be rerun safely and produces Gold datasets that directly answer all three analytical requirements.


