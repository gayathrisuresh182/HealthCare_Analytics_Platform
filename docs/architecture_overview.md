# Healthcare Analytics Platform - Architecture Overview

## 🏗️ **System Architecture**

### **High-Level Architecture**

```
┌─────────────┐
│  CSV Files  │
│  (Source)   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  AWS S3     │
│  (Bronze)   │
│  Data Lake  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Snowflake  │
│  Data       │
│  Warehouse  │
└──────┬──────┘
       │
       ├─────────────────┐
       │                 │
       ▼                 ▼
┌─────────────┐   ┌─────────────┐
│    dbt      │   │   Great     │
│ Transform   │   │ Expectations│
│             │   │  Validation │
└──────┬──────┘   └──────┬──────┘
       │                 │
       ▼                 ▼
┌─────────────┐   ┌─────────────┐
│   Marts     │   │  Data Docs  │
│  (Gold)     │   │  & Reports  │
└──────┬──────┘   └─────────────┘
       │
       ▼
┌─────────────┐
│  BI Tools   │
│  (Tableau)  │
└─────────────┘
```

---

## 📊 **Data Flow (ELT Pattern)**

### **1. Extract & Load**
- **Source**: 3 CSV files (CMS healthcare data)
- **Storage**: AWS S3 Bronze layer
- **Loading**: Snowflake external stage → `raw` schema

### **2. Transform (dbt)**
- **Staging Layer** (`raw_staging` schema):
  - Clean and standardize data
  - Handle data type conversions
  - Materialized as **VIEWS**
  
- **Intermediate Layer** (`raw_intermediate` schema):
  - Business logic and calculations
  - Window functions and aggregations
  - Materialized as **VIEWS**
  
- **Marts Layer** (`raw_marts` schema):
  - Dimensional star schema
  - Dimensions and facts
  - Materialized as **TABLES**

### **3. Validate (Great Expectations)**
- Data quality validation
- Business rule checks
- Data profiling
- Quality scorecards

### **4. Consume (BI Tools)**
- Tableau dashboards
- Custom SQL queries
- Self-service analytics

---

## 🗂️ **Snowflake Schema Structure**

### **Schema Layers**

```
HEALTHCARE_ANALYTICS/
├── raw/                    # Bronze Layer
│   ├── ipps_charges        # Raw charge data
│   ├── hospital_general_info  # Raw hospital data
│   └── readmissions        # Raw readmission data
│
├── raw_staging/            # Silver Layer - Cleaned
│   ├── stg_ipps_charges    # Cleaned charges (VIEW)
│   ├── stg_hospitals       # Cleaned hospitals (VIEW)
│   └── stg_readmissions    # Cleaned readmissions (VIEW)
│
├── raw_intermediate/        # Silver Layer - Business Logic
│   ├── int_charges_quality_merged
│   ├── int_hospital_cost_metrics
│   └── int_readmission_analysis
│
├── raw_marts/              # Gold Layer - Dimensional Model
│   ├── dim_hospitals       # Hospital dimension (TABLE)
│   ├── dim_drg_codes      # DRG dimension (TABLE)
│   ├── dim_geography       # Geography dimension (TABLE)
│   ├── dim_dates           # Date dimension (TABLE)
│   ├── fct_inpatient_charges  # Charges fact (TABLE)
│   ├── fct_readmissions    # Readmissions fact (TABLE)
│   ├── fct_hospital_summary # Hospital summary (TABLE)
│   └── fct_state_summary   # State summary (TABLE)
│
└── snapshots/              # SCD Type 2 Tracking
    └── hospitals_snapshot  # Historical hospital changes
```

---

## 🎯 **Dimensional Model (Star Schema)**

### **Fact Tables**

#### **`fct_inpatient_charges`** (Detail Fact)
- **Grain**: One row per hospital per DRG code
- **Measures**: 
  - Total discharges
  - Average covered charges
  - Average Medicare payments
  - Total payments
  - Markup ratios
- **Dimensions**: Hospital, DRG, Geography
- **Records**: ~146,294

#### **`fct_readmissions`** (Detail Fact)
- **Grain**: One row per hospital per readmission measure
- **Measures**:
  - Number of discharges
  - Number of readmissions
  - Excess readmission ratio
  - Observed readmission rate
- **Dimensions**: Hospital, Geography, Date
- **Records**: ~8,121

#### **`fct_hospital_summary`** (Aggregated Fact)
- **Grain**: One row per hospital
- **Measures**: Aggregated totals across all DRGs
- **Records**: ~2,675

#### **`fct_state_summary`** (Aggregated Fact)
- **Grain**: One row per state
- **Measures**: State-level aggregations
- **Records**: ~53

### **Dimension Tables**

#### **`dim_hospitals`**
- **Grain**: One row per hospital per version (SCD Type 2)
- **Attributes**: 
  - Facility ID, name, address
  - Hospital type, ownership, rating
  - Quality measures
- **Records**: ~5,421+ (with historical versions)

#### **`dim_drg_codes`**
- **Grain**: One row per DRG code
- **Attributes**: DRG code, description, category
- **Records**: ~534

#### **`dim_geography`**
- **Grain**: One row per unique location
- **Attributes**: State, county, city, urban/rural classification
- **Records**: ~5,222

#### **`dim_dates`**
- **Grain**: One row per date
- **Attributes**: Date, year, quarter, month, day of week
- **Records**: ~1,461

---

## 🔄 **Data Quality Framework**

### **dbt Tests (58+ tests)**
- **Schema Tests**: Unique, not null, relationships
- **Custom Tests**: Business rules, value ranges
- **Test Coverage**: All primary keys, foreign keys, critical fields

### **Great Expectations**
- **Expectation Suites**: 5 suites covering marts tables
- **Validation**: Automated checks on data quality
- **Data Docs**: HTML documentation with validation results
- **Quality Scorecard**: Overall data quality metrics

### **Data Quality Flags**
- Track issues without altering data
- Flags for: covered charges issues, capped values, orphaned records
- Preserve original values for analysis

---

## 🚀 **CI/CD Pipeline**

### **GitHub Actions Workflow**

```
On Push/PR:
  ├─ Install dependencies
  ├─ Run dbt compile
  ├─ Run dbt parse
  ├─ Run dbt tests
  └─ Upload test results

On Push to 'develop':
  ├─ Run dbt (dev environment)
  └─ Run Great Expectations

On Push to 'main':
  ├─ Run dbt (prod environment)
  └─ Run Great Expectations
```

### **Branch Strategy**
- **`main`**: Production environment
- **`develop`**: Development environment
- **Feature branches**: Development and testing

---

## 📁 **Project Structure**

```
HealthCare_Analytics_Platform/
├── models/
│   ├── staging/          # Staging models (views)
│   ├── intermediate/    # Intermediate models (views)
│   └── marts/            # Mart models (tables)
├── snapshots/            # SCD Type 2 snapshots
├── tests/                # Custom data tests
├── macros/               # Reusable macros
├── analyses/             # Ad-hoc analysis queries
├── scripts/              # Automation scripts
├── gx/                   # Great Expectations config
├── docs/                 # Documentation
└── .github/workflows/    # CI/CD pipelines
```

---

## 🔐 **Security & Access**

### **Snowflake**
- **Database**: `HEALTHCARE_ANALYTICS`
- **Schemas**: `raw`, `raw_staging`, `raw_intermediate`, `raw_marts`, `snapshots`
- **Warehouse**: `transforming_wh`
- **Role**: `ACCOUNTADMIN` (for setup)

### **AWS S3**
- **Bucket**: `healthcare-analytics-datalake-gayat-2026`
- **Structure**: Bronze/Silver/Gold medallion architecture
- **Access**: Snowflake external stage with IAM role

---

## 📊 **Data Volume**

- **Raw Data**: ~3 CSV files
- **Staging Views**: 3 views
- **Intermediate Views**: 3 views
- **Mart Tables**: 8 tables
- **Total Fact Records**: ~154,415+ rows
- **Total Dimension Records**: ~12,000+ rows

---

## 🎯 **Key Design Decisions**

### **1. ELT vs ETL**
- **Choice**: ELT (Extract, Load, Transform)
- **Reason**: Transform in Snowflake for scalability and cost efficiency

### **2. Views vs Tables**
- **Staging/Intermediate**: Views (flexibility, always fresh)
- **Marts**: Tables (performance, pre-aggregated)

### **3. Star Schema**
- **Choice**: Dimensional modeling
- **Reason**: Optimized for analytics and BI tools

### **4. SCD Type 2**
- **Choice**: Track historical changes
- **Reason**: Preserve audit trail for hospital attributes

### **5. Data Quality Flags**
- **Choice**: Track issues, don't fail
- **Reason**: Preserve original data, enable analysis of issues

---

## 🔗 **Technology Stack**

- **Cloud**: AWS (S3)
- **Data Warehouse**: Snowflake
- **Transformations**: dbt Core
- **Data Quality**: Great Expectations
- **BI Tools**: Tableau
- **CI/CD**: GitHub Actions
- **Version Control**: Git

---

## 📈 **Performance Considerations**

- **Materialization Strategy**: Views for staging/intermediate, Tables for marts
- **Concurrency**: 4-8 threads depending on environment
- **Query Tagging**: Track queries by environment
- **Warehouse Sizing**: `transforming_wh` for transformations

---

This architecture supports scalable, maintainable, and production-ready healthcare analytics.

