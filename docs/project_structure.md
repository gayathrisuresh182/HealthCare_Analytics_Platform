# Project Structure - Healthcare Analytics Platform

## 📁 **Directory Structure**

```
HealthCare_Analytics_Platform/
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml              # CI/CD pipeline
│
├── analyses/                        # Ad-hoc analysis queries
│   ├── cost_quality_correlation.sql
│   ├── geographic_efficiency.sql
│   └── ownership_impact_analysis.sql
│
├── data/                           # Source CSV files (local)
│   ├── hospital_general_info.csv
│   ├── ipps_charges.csv
│   └── readmissions.csv
│
├── docs/                           # Documentation
│   ├── architecture_overview.md
│   ├── data_lake_structure.md
│   ├── project_structure.md (this file)
│   ├── comprehensive_data_analytics_guide.md
│   ├── great_expectations_implementation.md
│   ├── pipeline_automation_guide.md
│   ├── project_completion_summary.md
│   ├── snapshots_guide.md
│   └── tableau_setup_guide.md
│
├── dbt_packages/                   # dbt package dependencies
│   └── dbt_utils/                  # Utility macros
│
├── gx/                             # Great Expectations
│   ├── checkpoints/
│   │   └── marts_checkpoint.yml
│   ├── expectations/
│   │   └── marts/
│   ├── great_expectations.yml
│   └── uncommitted/
│       └── data_docs/
│
├── logs/                           # dbt logs
│   └── dbt.log
│
├── macros/                         # Reusable dbt macros
│   └── custom_tests.sql
│
├── models/                         # dbt models
│   ├── intermediate/
│   │   ├── int_charges_quality_merged.sql
│   │   ├── int_hospital_cost_metrics.sql
│   │   ├── int_readmission_analysis.sql
│   │   └── schema.yml
│   ├── marts/
│   │   ├── dim_dates.sql
│   │   ├── dim_drg_codes.sql
│   │   ├── dim_geography.sql
│   │   ├── dim_hospitals.sql
│   │   ├── fct_hospital_summary.sql
│   │   ├── fct_inpatient_charges.sql
│   │   ├── fct_readmissions.sql
│   │   ├── fct_state_summary.sql
│   │   └── schema.yml
│   └── staging/
│       ├── schema.yml
│       ├── sources.yml
│       ├── stg_hospitals.sql
│       ├── stg_ipps_charges.sql
│       └── stg_readmissions.sql
│
├── scripts/                        # Automation and utility scripts
│   ├── gx_*.py                     # Great Expectations scripts
│   ├── run_pipeline*.py            # Pipeline automation
│   ├── tableau_custom_sql_*.sql    # Tableau queries
│   └── *.sql                       # SQL utilities
│
├── snapshots/                      # SCD Type 2 snapshots
│   └── hospitals_snapshot.sql
│
├── target/                         # dbt compilation output
│
├── tests/                          # Custom data tests
│   ├── assert_business_rules.sql
│   ├── assert_positive_charges.sql
│   ├── assert_readmission_ratio_valid.sql
│   └── assert_valid_ratings.sql
│
├── .gitignore                      # Git ignore rules
├── dbt_project.yml                 # dbt project configuration
├── packages.yml                    # dbt package dependencies
├── profiles.yml.template          # dbt connection template
├── QUICK_START.md                  # Quick start guide
├── README_GX_ENHANCEMENTS.md       # GX enhancements guide
└── Readme.md                       # Main project README
```

---

## 📂 **Directory Purposes**

### **`.github/workflows/`**
- **Purpose**: CI/CD pipeline definitions
- **Files**: GitHub Actions workflows
- **Key File**: `dbt_ci.yml` - Automated testing and deployment

### **`analyses/`**
- **Purpose**: Ad-hoc analysis queries (not part of dbt pipeline)
- **Use Case**: Exploratory analysis, one-off queries
- **Files**: SQL analysis queries

### **`data/`**
- **Purpose**: Local source CSV files
- **Note**: These are loaded to S3 and Snowflake
- **Files**: 3 CSV files (charges, hospitals, readmissions)

### **`docs/`**
- **Purpose**: Project documentation
- **Contents**: Architecture, setup guides, best practices
- **Key Files**: Architecture overview, data lake structure, setup guides

### **`gx/`**
- **Purpose**: Great Expectations configuration
- **Contents**: Expectation suites, checkpoints, data docs
- **Key Files**: `great_expectations.yml`, checkpoints, expectations

### **`macros/`**
- **Purpose**: Reusable dbt macros
- **Files**: Custom test macros, utility functions

### **`models/`**
- **Purpose**: dbt transformation models
- **Structure**:
  - `staging/`: Clean and standardize (views)
  - `intermediate/`: Business logic (views)
  - `marts/`: Dimensional model (tables)
- **Files**: SQL models + schema.yml for tests

### **`scripts/`**
- **Purpose**: Automation and utility scripts
- **Categories**:
  - Great Expectations scripts (`gx_*.py`)
  - Pipeline automation (`run_pipeline*.py`)
  - Tableau SQL queries (`tableau_custom_sql_*.sql`)
  - SQL utilities

### **`snapshots/`**
- **Purpose**: SCD Type 2 historical tracking
- **Files**: Snapshot definitions

### **`tests/`**
- **Purpose**: Custom data quality tests
- **Files**: SQL test queries

---

## 🔧 **Configuration Files**

### **`dbt_project.yml`**
- **Purpose**: dbt project configuration
- **Contents**: 
  - Model materialization (views vs tables)
  - Schema mappings
  - Variables
  - Package dependencies

### **`packages.yml`**
- **Purpose**: dbt package dependencies
- **Contents**: `dbt_utils` package

### **`profiles.yml.template`**
- **Purpose**: Template for Snowflake connection
- **Usage**: Copy to `~/.dbt/profiles.yml` and fill in credentials

### **`.gitignore`**
- **Purpose**: Exclude files from Git
- **Contents**: `target/`, `logs/`, `dbt_packages/`, credentials

---

## 🎯 **Model Organization**

### **Staging Models** (`models/staging/`)
- **Naming**: `stg_*`
- **Purpose**: Clean and standardize raw data
- **Materialization**: Views
- **Schema**: `raw_staging`

### **Intermediate Models** (`models/intermediate/`)
- **Naming**: `int_*`
- **Purpose**: Business logic, calculations, joins
- **Materialization**: Views
- **Schema**: `raw_intermediate`

### **Mart Models** (`models/marts/`)
- **Naming**: `dim_*` (dimensions), `fct_*` (facts)
- **Purpose**: Analytics-ready dimensional model
- **Materialization**: Tables
- **Schema**: `raw_marts`

---

## 📊 **File Naming Conventions**

### **Models:**
- **Staging**: `stg_<source_name>.sql`
- **Intermediate**: `int_<purpose>.sql`
- **Dimensions**: `dim_<entity>.sql`
- **Facts**: `fct_<entity>.sql`

### **Tests:**
- **Custom tests**: `assert_<rule>.sql`
- **Schema tests**: Defined in `schema.yml`

### **Scripts:**
- **Great Expectations**: `gx_<action>.py`
- **Pipeline**: `run_pipeline*.py` or `.sh` or `.bat`
- **SQL utilities**: `<purpose>.sql`

---

## 🔗 **Dependencies**

### **dbt Packages:**
- `dbt_utils` - Utility macros for tests and transformations

### **Python Packages:**
- `dbt-snowflake` - dbt adapter for Snowflake
- `great-expectations` - Data quality framework
- `snowflake-sqlalchemy` - SQLAlchemy driver for Snowflake

---

## 📝 **Documentation Files**

### **Essential Documentation:**
- `docs/architecture_overview.md` - System architecture
- `docs/data_lake_structure.md` - Data lake organization
- `docs/project_structure.md` - This file

---

## 🚀 **Workflow**

### **Development:**
1. Edit models in `models/`
2. Run `dbt run` to test
3. Run `dbt test` to validate
4. Commit to Git

### **CI/CD:**
1. Push to GitHub
2. GitHub Actions runs tests
3. On `main` branch → Production deployment
4. On `develop` branch → Dev deployment

### **Data Quality:**
1. Run `dbt test` for schema tests
2. Run `python scripts/gx_run_checkpoint.py` for GX validation
3. Review data docs

---

This structure supports maintainable, scalable, and production-ready data engineering.

