# Healthcare Analytics Platform

## Project Overview

This is a production-grade healthcare analytics platform demonstrating modern data engineering skills including cloud data warehousing, dimensional modeling, dbt transformations, and multi-tool BI implementation.

## Architecture

### Data Flow (ELT Pattern)

```
CSV Files → S3 (Bronze) → Snowflake raw schema → dbt Staging → dbt Intermediate → dbt Marts → BI Tools
```

### Snowflake Schema Layers

1. **`raw`** (Bronze Layer)
   - Exact CSV data with minimal transformation
   - Tables: `ipps_charges`, `hospital_general_info`, `readmissions`

2. **`staging`** (Silver Layer - Cleaned)
   - Cleaned, standardized data
   - Same grain as raw
   - Models: `stg_ipps_charges`, `stg_hospitals`, `stg_readmissions`
   - Materialized as: **VIEW**

3. **`intermediate`** (Silver Layer - Business Logic)
   - Joined datasets, complex calculations
   - Advanced SQL with window functions
   - Models: `int_charges_quality_merged`, `int_hospital_cost_metrics`, `int_readmission_analysis`
   - Materialized as: **VIEW**

4. **`marts`** (Gold Layer - Dimensional Model)
   - Final star schema for analytics
   - Dimensions and facts
   - Materialized as: **TABLE**

5. **`snapshots`** (SCD Type 2 Tracking)
   - Historical tracking of dimension changes
   - Model: `hospitals_snapshot`

## Dimensional Model (Star Schema)

### Dimensions

- **`dim_hospitals`** (5,421+ rows with SCD Type 2)
  - Tracks: ownership changes, rating changes, service additions
  - Source: `stg_hospitals` + `hospitals_snapshot`
  - Grain: One row per hospital per version

- **`dim_drg_codes`** (~300 rows)
  - DRG codes with category classifications
  - Source: `stg_ipps_charges` (extract unique DRG codes)
  - Grain: One row per DRG code

- **`dim_geography`** (~3,000 rows)
  - State, county, urban/rural classification
  - Source: `stg_ipps_charges` + `stg_hospitals`
  - Includes: Census regions, RUCA codes
  - Grain: One row per unique geography combination

- **`dim_dates`** (~1,500 rows)
  - Date spine: 2020-01-01 to 2023-12-31
  - Includes: fiscal year, quarters, flags
  - Grain: One row per date

### Detail Facts

- **`fct_inpatient_charges`** (146,427 rows)
  - Grain: hospital × DRG code
  - Source: `stg_ipps_charges`
  - Measures: discharges, charges, payments, markup ratio, revenue

- **`fct_readmissions`** (18,510 rows)
  - Grain: hospital × readmission measure
  - Source: `stg_readmissions`
  - Measures: readmission rates, excess ratios, case counts

### Aggregated Facts

- **`fct_hospital_summary`** (5,421 rows)
  - Grain: hospital
  - Aggregated FROM: `fct_inpatient_charges` + `fct_readmissions`
  - Hospital-level summary metrics

- **`fct_state_summary`** (51 rows)
  - Grain: state
  - Aggregated FROM: `fct_hospital_summary`
  - State-level summary metrics

## Datasets

### Dataset 1: Medicare Inpatient Hospitals - by Provider and Service
- **File:** `ipps_charges.csv`
- **Rows:** ~146,427
- **Grain:** One row per hospital per DRG procedure code
- **Join Key:** `Rndrng_Prvdr_CCN` (Hospital ID)

### Dataset 2: Hospital General Information
- **File:** `hospital_general_info.csv`
- **Rows:** ~5,421
- **Grain:** One row per hospital (master list)
- **Join Key:** `Facility ID` (Hospital ID)

### Dataset 3: Hospital Readmissions Reduction Program
- **File:** `readmissions.csv`
- **Rows:** ~18,510
- **Grain:** One row per hospital per readmission measure
- **Join Key:** `Facility ID` (Hospital ID)

## SQL Skills Demonstrated

### Advanced SQL Features

1. **Multi-level CTEs** (3+ levels in intermediate models)
2. **Window Functions** (10+ different types):
   - `RANK()`, `DENSE_RANK()`, `ROW_NUMBER()`
   - `NTILE()` for percentiles
   - `PERCENTILE_CONT()` for medians
   - `LAG()`, `LEAD()` for trend analysis
   - `SUM() OVER`, `AVG() OVER` for running totals
   - `FIRST_VALUE()`, `LAST_VALUE()`
   - `STDDEV() OVER` for statistical analysis

3. **Statistical Functions**:
   - `STDDEV()`, `VARIANCE()`
   - `CORR()` for correlation analysis

4. **Complex Joins**:
   - LEFT JOINs to preserve all hospitals
   - Handling unmatched records with COALESCE

## Project Structure

```
healthcare_analytics/
├── dbt_project.yml              # dbt project configuration
├── profiles.yml.template        # Snowflake connection template
├── packages.yml                 # dbt packages (dbt_utils)
├── .gitignore                   # Git ignore rules
│
├── models/
│   ├── staging/
│   │   ├── sources.yml          # Source definitions
│   │   ├── schema.yml           # Staging model tests
│   │   ├── stg_ipps_charges.sql
│   │   ├── stg_hospitals.sql
│   │   └── stg_readmissions.sql
│   │
│   ├── intermediate/
│   │   ├── schema.yml           # Intermediate model tests
│   │   ├── int_charges_quality_merged.sql
│   │   ├── int_hospital_cost_metrics.sql    # SHOWCASE WINDOW FUNCTIONS
│   │   └── int_readmission_analysis.sql
│   │
│   └── marts/
│       ├── schema.yml           # Mart model tests
│       ├── dim_hospitals.sql
│       ├── dim_drg_codes.sql
│       ├── dim_geography.sql
│       ├── dim_dates.sql
│       ├── fct_inpatient_charges.sql
│       ├── fct_readmissions.sql
│       ├── fct_hospital_summary.sql
│       └── fct_state_summary.sql
│
├── snapshots/
│   └── hospitals_snapshot.sql   # SCD Type 2 tracking
│
├── tests/
│   ├── assert_positive_charges.sql
│   ├── assert_valid_ratings.sql
│   ├── assert_readmission_ratio_valid.sql
│   └── assert_business_rules.sql
│
├── macros/
│   └── custom_tests.sql         # Reusable test macros
│
└── analyses/
    ├── cost_quality_correlation.sql
    ├── geographic_efficiency.sql
    └── ownership_impact_analysis.sql
```

## Setup Instructions

### 1. Prerequisites

- Python 3.11+
- dbt Core or dbt Cloud
- Snowflake account
- AWS S3 bucket (for data lake)

### 2. Install Dependencies

```bash
# Install dbt
pip install dbt-snowflake

# Install dbt packages
dbt deps
```

### 3. Configure Snowflake Connection

1. Copy `profiles.yml.template` to `~/.dbt/profiles.yml` (Windows: `C:\Users\<username>\.dbt\profiles.yml`)
2. Update with your Snowflake credentials:
   - Account locator
   - Username and password
   - Database: `HEALTHCARE_ANALYTICS`
   - Warehouse: `COMPUTE_WH`
   - Schema: `raw` (for initial connection)

### 4. Load Data to Snowflake

Load the three CSV files into Snowflake `raw` schema:

```sql
-- Example: Load ipps_charges.csv
CREATE TABLE raw.ipps_charges AS
SELECT * FROM @your_s3_stage/ipps_charges.csv
(FILE_FORMAT => 'CSV_FORMAT');
```

### 5. Run dbt Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_ipps_charges

# Run with tests
dbt run --select +fct_inpatient_charges
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Quality Framework

### Schema Tests (in schema.yml files)

- `unique` and `not_null` on all primary keys
- `relationships` for all foreign keys
- `accepted_values` for categorical fields
- `dbt_utils.accepted_range` for numeric ranges

### Custom Data Tests (in tests/)

- Value ranges: payments > 0, ratings 1-5
- Business rules: covered_charges >= medicare_payments
- Completeness: critical fields not null
- Cross-field validation

**Target: 30+ tests, 100% pass rate**

## SCD Type 2 Implementation

The `hospitals_snapshot` model tracks historical changes to:
- Hospital ownership
- Hospital overall rating
- Emergency services availability

When these columns change, dbt creates a new version with:
- `dbt_valid_from`: When the version became valid
- `dbt_valid_to`: When the version expired (9999-12-31 for current)
- `dbt_scd_id`: Unique identifier for the version
- `is_current`: Flag for current version

## BI Tools Integration

### Tableau
- Connect to Snowflake `marts` schema
- Use fact and dimension tables for dashboards
- Recommended: Hospital cost analysis, quality metrics, geographic comparisons

### Power BI
- Import from Snowflake `marts` schema
- Use aggregated facts (`fct_hospital_summary`, `fct_state_summary`) for performance
- Recommended: Executive dashboards, state-level comparisons

### Excel
- Use Power Query to connect to Snowflake
- Create pivot tables from fact tables
- Recommended: Ad-hoc analysis, detailed drill-downs

## Key Features

✅ **Cloud Infrastructure**: S3 + Snowflake  
✅ **ELT Pattern**: Extract → Load → Transform in warehouse  
✅ **Dimensional Modeling**: Star schema with dimensions and facts  
✅ **SCD Type 2**: Historical tracking with snapshots  
✅ **Advanced SQL**: CTEs, window functions, statistical functions  
✅ **Data Quality**: Comprehensive testing framework (30+ tests)  
✅ **Documentation**: Professional-grade schema documentation  
✅ **Aggregated Facts**: Built FROM detail facts using dbt refs  

## Development Workflow

1. **Load raw data** to Snowflake `raw` schema
2. **Run staging models**: `dbt run --select staging`
3. **Run intermediate models**: `dbt run --select intermediate`
4. **Run marts**: `dbt run --select marts`
5. **Run tests**: `dbt test`
6. **Generate docs**: `dbt docs generate && dbt docs serve`

## Next Steps

### Completed ✅
- [x] Load CSV files to Snowflake raw schema
- [x] Configure Snowflake connection in profiles.yml
- [x] Run `dbt deps` to install packages
- [x] Run `dbt run` to build all models
- [x] Run `dbt test` to validate data quality
- [x] Implement data quality flags and tracking
- [x] Fix data quality issues and business rules

### Recommended Next Steps 🚀

#### 1. **Great Expectations Setup** (Recommended)
- [ ] Install Great Expectations: `pip install great-expectations`
- [ ] Initialize GX project: `great_expectations init`
- [ ] Configure Snowflake datasource
- [ ] Create expectation suites for marts layer
- [ ] Set up checkpoints and data docs
- [ ] Integrate with dbt workflow

**See:** `docs/great_expectations_setup.md` for detailed guide

#### 2. **BI Tools Integration**
- [ ] Connect BI tools to marts schema
- [ ] Create dashboards in Tableau/Power BI/Excel
- [ ] Build executive dashboards
- [ ] Create operational reports

#### 3. **Monitoring & Alerting**
- [ ] Set up data quality monitoring
- [ ] Configure alerts for test failures
- [ ] Create data quality dashboard
- [ ] Set up automated reporting

#### 4. **Advanced Features**
- [ ] Implement data lineage tracking
- [ ] Set up CI/CD pipeline
- [ ] Create data catalog
- [ ] Implement data governance policies

## Skills Demonstrated

1. **Snowflake**: Multi-schema warehouse design
2. **dbt**: 20+ models with proper dependencies
3. **Dimensional Modeling**: Star + snowflake schemas
4. **SCD Type 2**: Historical tracking with snapshots
5. **Advanced SQL**: CTEs, window functions, statistical functions
6. **Data Quality**: Comprehensive testing framework (59+ tests, data quality flags)
7. **Documentation**: Professional-grade docs
8. **Great Expectations**: Ready for implementation (see `docs/great_expectations_setup.md`)

## License

This is a portfolio project for demonstration purposes.
