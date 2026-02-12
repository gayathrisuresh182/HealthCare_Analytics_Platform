# Dimension Models Architecture

This document provides a visual representation of the dimension models in the Healthcare Analytics Platform.

## Dimension Models Overview

### Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RAW DATA SOURCES                         │
│  (raw.ipps_charges, raw.hospital_general_info, raw.readmissions)│
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER (stg_*)                      │
├─────────────────────────────────────────────────────────────────┤
│  stg_ipps_charges          │  stg_hospitals  │  stg_readmissions│
└──────────┬─────────────────┴────────┬─────────┴──────────────────┘
           │                          │
           │                          │
           ▼                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DIMENSION MODELS (dim_*)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐                     │
│  │  dim_hospitals   │  │  dim_drg_codes  │                     │
│  ├──────────────────┤  ├──────────────────┤                     │
│  │ PK: hospital_key │  │ PK: drg_key     │                     │
│  │ • facility_id    │  │ • drg_code      │                     │
│  │ • facility_name  │  │ • drg_desc      │                     │
│  │ • hospital_type  │  │ • category      │                     │
│  │ • ownership      │  │ • category_desc │                     │
│  │ • rating         │  └──────────────────┘                     │
│  │ • quality metrics│         ▲                                 │
│  │ • SCD Type 2     │         │                                 │
│  └──────────────────┘         │                                 │
│         ▲                     │                                 │
│         │                     │                                 │
│         │            ┌────────┴────────┐                        │
│         │            │                 │                        │
│  ┌──────┴──────┐  ┌──┴──────────┐  ┌──┴──────────┐            │
│  │dim_geography│  │ dim_dates   │  │             │            │
│  ├─────────────┤  ├─────────────┤  │             │            │
│  │PK: geo_key  │  │PK: date_key │  │             │            │
│  │• state      │  │• date_day   │  │             │            │
│  │• city       │  │• year       │  │             │            │
│  │• zip_code   │  │• month      │  │             │            │
│  │• county     │  │• quarter    │  │             │            │
│  │• RUCA code  │  │• fiscal_yr  │  │             │            │
│  │• urban/rural│ │• is_weekend │  │             │            │
│  │• census_reg │ │• flags      │  │             │            │
│  └─────────────┘  └─────────────┘  │             │            │
│                                     │             │            │
└─────────────────────────────────────┴─────────────┴────────────┘
           │              │              │              │
           │              │              │              │
           ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FACT TABLES (fct_*)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────┐  ┌──────────────────────────┐     │
│  │ fct_inpatient_charges    │  │   fct_readmissions       │     │
│  ├──────────────────────────┤  ├──────────────────────────┤     │
│  │ PK: charge_key           │  │ PK: readmission_key      │     │
│  │ FK: hospital_key ────────┼──┼─► hospital_key           │     │
│  │ FK: drg_key ─────────────┼──┼─► geography_key         │     │
│  │ FK: geography_key ───────┼──┼─► start_date_key         │     │
│  │ • total_discharges       │  │ FK: end_date_key         │     │
│  │ • avg_covered_charges    │  │ • number_of_discharges   │     │
│  │ • avg_total_payment      │  │ • excess_readmission_ratio│    │
│  │ • avg_medicare_payment   │  │ • performance_category   │     │
│  │ • markup_ratio           │  └──────────────────────────┘     │
│  └──────────────────────────┘            │                       │
│           │                              │                       │
│           │                              │                       │
│           ▼                              ▼                       │
│  ┌──────────────────────────┐  ┌──────────────────────────┐     │
│  │  fct_hospital_summary    │  │    fct_state_summary    │     │
│  ├──────────────────────────┤  ├──────────────────────────┤     │
│  │ PK: hospital_key         │  │ PK: state_summary_key     │     │
│  │ • total_discharges       │  │ • state_abbreviation     │     │
│  │ • total_covered_charges  │  │ • census_region         │     │
│  │ • avg_markup_ratio       │  │ • hospital_count        │     │
│  │ • weighted_excess_ratio  │  │ • state_avg_charges     │     │
│  └──────────────────────────┘  │ • state_avg_readmission │     │
│                                 └──────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Dimension to Fact Relationships

```
┌──────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA RELATIONSHIPS                     │
└──────────────────────────────────────────────────────────────────┘

                    ┌──────────────┐
                    │dim_hospitals │
                    │(hospital_key)│
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│fct_inpatient  │  │fct_readmissions│  │fct_hospital   │
│   _charges    │  │                │  │   _summary    │
└───────┬───────┘  └────────┬───────┘  └───────────────┘
        │                   │
        │                   │
        ▼                   ▼
┌───────────────┐  ┌───────────────┐
│dim_drg_codes  │  │dim_geography  │
│  (drg_key)    │  │(geography_key)│
└───────────────┘  └───────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │dim_dates    │
                    │(date_key)   │
                    └──────────────┘
```

### Detailed Dimension Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                        dim_hospitals                            │
├─────────────────────────────────────────────────────────────────┤
│ Primary Key: hospital_key (surrogate)                           │
│ Natural Key: facility_id                                         │
│                                                                  │
│ Attributes:                                                     │
│   • facility_name, address, city, state, zip_code, county       │
│   • hospital_type, hospital_ownership                           │
│   • emergency_services, birthing_friendly                       │
│   • hospital_overall_rating (1-5 stars)                         │
│   • Quality Metrics:                                            │
│     - mort_measures_better/worse/no_different                    │
│     - safety_measures_better/worse/no_different                 │
│     - readm_measures_better/worse/no_different                  │
│   • SCD Type 2: valid_from, valid_to, is_current               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        dim_drg_codes                            │
├─────────────────────────────────────────────────────────────────┤
│ Primary Key: drg_key (surrogate)                                │
│ Natural Key: drg_code                                           │
│                                                                  │
│ Attributes:                                                     │
│   • drg_code (e.g., '003', '023')                               │
│   • drg_description                                             │
│   • drg_category_code (first 1-2 digits)                        │
│   • drg_category_description (20+ categories):                  │
│     - Nervous System, Circulatory, Respiratory, etc.             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        dim_geography                            │
├─────────────────────────────────────────────────────────────────┤
│ Primary Key: geography_key (surrogate)                          │
│                                                                  │
│ Attributes:                                                     │
│   • state_abbreviation, state_fips_code                          │
│   • city, zip_code, county                                      │
│   • ruca_code (1-10)                                            │
│   • ruca_description                                            │
│   • urban_rural_classification:                                 │
│     - Urban, Large Rural, Small Rural, Remote Rural             │
│   • census_region: Northeast, Midwest, South, West              │
│   • census_division: 9 divisions (New England, etc.)            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         dim_dates                               │
├─────────────────────────────────────────────────────────────────┤
│ Primary Key: date_key (date)                                    │
│ Date Range: 2020-01-01 to 2023-12-31                           │
│                                                                  │
│ Attributes:                                                     │
│   • date_day, year, month, day, quarter, week                   │
│   • day_name, month_name                                        │
│   • fiscal_year, fiscal_quarter (Oct 1 - Sep 30)               │
│   • Flags: is_weekend, is_weekday                               │
│   • Flags: is_first_of_month, is_last_of_month                 │
│   • Flags: is_first_of_year, is_last_of_year                   │
└─────────────────────────────────────────────────────────────────┘
```

## Dimension Model Details

### 1. dim_hospitals
**Purpose:** Master hospital dimension with SCD Type 2 support for historical tracking

**Key Attributes:**
- **Primary Key:** `hospital_key` (surrogate key)
- **Natural Key:** `facility_id`
- **SCD Fields:** `valid_from`, `valid_to`, `is_current`
- **Quality Metrics:** Mortality, Safety, Readmission, Patient Experience measures
- **Source:** `stg_hospitals` + snapshots (for SCD Type 2)

**Relationships:**
- One-to-Many with `fct_inpatient_charges`
- One-to-Many with `fct_readmissions`
- One-to-One with `fct_hospital_summary`

---

### 2. dim_drg_codes
**Purpose:** Master list of DRG (Diagnosis Related Group) codes with category classifications

**Key Attributes:**
- **Primary Key:** `drg_key` (surrogate key)
- **Natural Key:** `drg_code`
- **Category Classification:** 20+ DRG categories (Nervous System, Circulatory, etc.)
- **Source:** Extracted unique DRG codes from `stg_ipps_charges`

**Relationships:**
- One-to-Many with `fct_inpatient_charges`

---

### 3. dim_geography
**Purpose:** Master geography dimension with urban/rural classification and census regions

**Key Attributes:**
- **Primary Key:** `geography_key` (surrogate key)
- **Geographic Levels:** State, City, ZIP Code, County
- **RUCA Classification:** Urban, Large Rural, Small Rural, Remote Rural
- **Census Regions:** Northeast, Midwest, South, West
- **Census Divisions:** 9 divisions (New England, Middle Atlantic, etc.)
- **Source:** Combined from `stg_ipps_charges` and `stg_hospitals`

**Relationships:**
- One-to-Many with `fct_inpatient_charges`
- One-to-Many with `fct_readmissions`
- One-to-Many with `fct_state_summary`

---

### 4. dim_dates
**Purpose:** Date dimension table for time-based analysis (2020-2023)

**Key Attributes:**
- **Primary Key:** `date_key` (date)
- **Date Components:** Year, Month, Day, Quarter, Week
- **Fiscal Year:** Oct 1 - Sep 30 fiscal calendar
- **Flags:** `is_weekend`, `is_weekday`, `is_first_of_month`, etc.
- **Source:** Generated date spine (2020-01-01 to 2023-12-31)

**Relationships:**
- One-to-Many with `fct_readmissions` (via `start_date_key` and `end_date_key`)

---

## Data Flow Summary

```
Raw Data (sources.yml)
    ↓
Staging Models (stg_*)
    ↓
Dimension Models (dim_*)
    ↓
Fact Tables (fct_*)
    ↓
Summary Fact Tables (fct_*_summary)
```

## Star Schema Structure

The marts layer follows a **star schema** design pattern:

- **Dimensions:** `dim_hospitals`, `dim_drg_codes`, `dim_geography`, `dim_dates`
- **Facts:** `fct_inpatient_charges`, `fct_readmissions`
- **Aggregated Facts:** `fct_hospital_summary`, `fct_state_summary`

Each fact table references dimension tables via foreign keys (surrogate keys) for efficient joins and analysis.

