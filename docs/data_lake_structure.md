# Data Lake Structure - Healthcare Analytics Platform

## 🏗️ **Medallion Architecture**

This project follows the **medallion architecture pattern** (Bronze → Silver → Gold) for data organization:

```
Bronze (Raw) → Silver (Cleaned) → Gold (Curated)
```

---

## 📦 **AWS S3 Data Lake Structure**

### **S3 Bucket: `healthcare-analytics-datalake-gayat-2026`**

```
s3://healthcare-analytics-datalake-gayat-2026/
│
├── bronze/                          # Raw, unprocessed data
│   └── raw/
│       └── cms-data/
│           ├── ipps_charges/
│           │   └── ipps_charges.csv
│           ├── hospital_general_info/
│           │   └── hospital_general_info.csv
│           └── readmissions/
│               └── readmissions.csv
│
├── silver/                          # Cleaned, validated data
│   └── processed/
│       ├── staging/                 # dbt staging outputs (optional)
│       ├── intermediate/           # dbt intermediate outputs (optional)
│       └── snapshots/              # dbt snapshot outputs (optional)
│
└── gold/                            # Curated, business-ready data
    └── curated/
        └── marts/                   # dbt marts outputs (optional)
```

---

## 🗄️ **Snowflake Schema Structure**

### **Database: `HEALTHCARE_ANALYTICS`**

```
HEALTHCARE_ANALYTICS/
│
├── raw/                             # Bronze Layer
│   ├── ipps_charges                 # Raw charge data (exact CSV structure)
│   ├── hospital_general_info        # Raw hospital data (exact CSV structure)
│   └── readmissions                 # Raw readmission data (exact CSV structure)
│
├── raw_staging/                     # Silver Layer - Cleaned
│   ├── stg_ipps_charges             # Cleaned charges (VIEW)
│   ├── stg_hospitals                # Cleaned hospitals (VIEW)
│   └── stg_readmissions             # Cleaned readmissions (VIEW)
│
├── raw_intermediate/                 # Silver Layer - Business Logic
│   ├── int_charges_quality_merged   # Charges + quality metrics (VIEW)
│   ├── int_hospital_cost_metrics     # Hospital cost analysis (VIEW)
│   └── int_readmission_analysis     # Readmission analysis (VIEW)
│
├── raw_marts/                       # Gold Layer - Dimensional Model
│   ├── dim_hospitals                # Hospital dimension (TABLE)
│   ├── dim_drg_codes                # DRG dimension (TABLE)
│   ├── dim_geography                # Geography dimension (TABLE)
│   ├── dim_dates                    # Date dimension (TABLE)
│   ├── fct_inpatient_charges        # Charges fact (TABLE)
│   ├── fct_readmissions             # Readmissions fact (TABLE)
│   ├── fct_hospital_summary         # Hospital summary (TABLE)
│   └── fct_state_summary            # State summary (TABLE)
│
└── snapshots/                       # SCD Type 2 Tracking
    └── hospitals_snapshot           # Historical hospital changes
```

---

## 📊 **Layer Descriptions**

### **Bronze Layer (`raw` schema)**
- **Purpose**: Store raw, unprocessed data exactly as received
- **Structure**: Matches source CSV files exactly
- **Data Types**: Mostly VARCHAR (minimal transformation)
- **Use Case**: Source of truth, audit trail
- **Tables**: 
  - `ipps_charges`
  - `hospital_general_info`
  - `readmissions`

### **Silver Layer (`raw_staging` + `raw_intermediate` schemas)**
- **Purpose**: Cleaned, standardized, and enriched data
- **Structure**: Same grain as bronze, but cleaned
- **Data Types**: Proper types (NUMERIC, DATE, etc.)
- **Use Case**: Data quality, business logic, calculations
- **Staging Models** (Views):
  - `stg_ipps_charges` - Cleaned charges
  - `stg_hospitals` - Cleaned hospitals
  - `stg_readmissions` - Cleaned readmissions
- **Intermediate Models** (Views):
  - `int_charges_quality_merged` - Charges + quality metrics
  - `int_hospital_cost_metrics` - Cost analysis with window functions
  - `int_readmission_analysis` - Readmission analysis

### **Gold Layer (`raw_marts` schema)**
- **Purpose**: Business-ready, dimensional model for analytics
- **Structure**: Star schema (dimensions + facts)
- **Data Types**: Optimized for analytics
- **Use Case**: BI tools, dashboards, reporting
- **Dimensions** (Tables):
  - `dim_hospitals` - Hospital master (SCD Type 2)
  - `dim_drg_codes` - DRG code master
  - `dim_geography` - Geography master
  - `dim_dates` - Date dimension
- **Facts** (Tables):
  - `fct_inpatient_charges` - Detail charges (hospital × DRG)
  - `fct_readmissions` - Detail readmissions (hospital × measure)
  - `fct_hospital_summary` - Hospital aggregations
  - `fct_state_summary` - State aggregations

### **Snapshots Layer (`snapshots` schema)**
- **Purpose**: Track historical changes (SCD Type 2)
- **Structure**: Current + historical versions
- **Use Case**: Audit trail, point-in-time analysis
- **Snapshots**:
  - `hospitals_snapshot` - Tracks hospital ownership, rating, services changes

---

## 🔄 **Data Flow**

### **1. Ingestion (Bronze)**
```
CSV Files → S3 Bronze → Snowflake External Stage → raw schema
```

### **2. Transformation (Silver)**
```
raw → dbt staging models → raw_staging (views)
raw_staging → dbt intermediate models → raw_intermediate (views)
```

### **3. Modeling (Gold)**
```
raw_staging + raw_intermediate → dbt marts models → raw_marts (tables)
```

### **4. Consumption**
```
raw_marts → BI Tools (Tableau) → Dashboards
```

---

## 📐 **Schema Naming Convention**

### **Why `raw_` prefix?**
- dbt uses `profiles.yml` default schema: `raw`
- Combined with `+schema` in `dbt_project.yml`, creates:
  - `raw_staging` (raw + staging)
  - `raw_intermediate` (raw + intermediate)
  - `raw_marts` (raw + marts)

### **Schema Purposes:**
- **`raw`**: Source of truth, unprocessed
- **`raw_staging`**: Cleaned, standardized
- **`raw_intermediate`**: Business logic, calculations
- **`raw_marts`**: Analytics-ready, dimensional
- **`snapshots`**: Historical tracking

---

## 🎯 **Materialization Strategy**

### **Views (Staging & Intermediate)**
- **Why**: Always fresh, no storage cost, flexible
- **Trade-off**: Slightly slower queries (recompute on access)

### **Tables (Marts)**
- **Why**: Fast queries, pre-aggregated, optimized
- **Trade-off**: Storage cost, needs refresh

---

## 📊 **Data Volume Estimates**

### **Bronze (Raw)**
- `ipps_charges`: ~146,294 rows
- `hospital_general_info`: ~5,421 rows
- `readmissions`: ~8,121 rows

### **Silver (Staging/Intermediate)**
- Same row counts as bronze (same grain)
- Views (no storage, computed on demand)

### **Gold (Marts)**
- **Dimensions**: ~12,000+ rows total
- **Facts**: ~154,415+ rows total
- **Tables** (stored, optimized)

---

## 🔐 **Access Patterns**

### **Read Access:**
- **BI Tools**: Read from `raw_marts` schema
- **Analysts**: Query `raw_staging` and `raw_intermediate` for ad-hoc analysis
- **Data Engineers**: Access all layers for debugging

### **Write Access:**
- **dbt**: Writes to staging, intermediate, marts, snapshots
- **External Stage**: Loads to `raw` schema
- **No direct writes** to marts (only via dbt)

---

## 🎯 **Design Principles**

1. **Separation of Concerns**: Each layer has a specific purpose
2. **Data Quality**: Issues caught and flagged, not hidden
3. **Auditability**: Raw data preserved, transformations documented
4. **Performance**: Views for flexibility, tables for speed
5. **Scalability**: Architecture supports growth

---

This structure provides a clear, maintainable, and scalable data architecture.

