# Sanlam FinTech - Marketing Analytics dbt Project

This dbt project provides a scalable, testable, and maintainable data pipeline for analyzing cross-platform advertising campaign performance.

Its primary purpose is to refactor the logic from a single monolithic SQL script (`campaignstats_cte.sql`) into a modular data architecture. This new structure solves for maintainability, reusability, and data quality.

## 📈 Project Architecture: The Medallion Model

This project follows a **Bronze, Silver, Gold** (Medallion) architecture. Each layer serves a distinct purpose, transforming raw data into analytics-ready assets.



### 1. `models/1.sources` (Source Declaration)

This directory **does not contain SQL models**. Instead, it holds the `.yml` configuration files that **declare and test our raw data sources** from the `RAW_DATA` database.

* **Purpose:** To define the "contract" with our raw data.
* **Key Actions:**
    * Defines source tables (e.g., `tiktok.ad_report_daily`, `google_ads.ad_performance_report`).
    * Tests raw data for **freshness**, ensuring our data loads are up-to-date.
    * Performs basic source-level data quality checks (e.g., `not_null` on primary keys).

### 2. `models/2.bronze` (Staging & Cleansing)

This is our **staging layer**. Models in this layer are 1:1 with their corresponding source tables.

* **Purpose:** To create a clean, standardized base layer of data.
* **Key Actions:**
    * **Renaming:** Converts source column names to a consistent `snake_case` convention (e.g., `stat_time_day` -> `report_date`).
    * **Type Casting:** Casts all columns to the correct data type (e.g., `spend` -> `float`, `date` -> `date`).
    * **Basic Cleansing:** Handles simple data cleansing.
    * **No joins** are performed at this layer. All models select from a single `{{ source() }}`.

### 3. `models/3.silver` (Intermediate & Conformed)

This is our **intermediate layer**. These models are responsible for applying business logic and joining different data sources to create reusable, conformed data assets.

* **Purpose:** To create a "single source of truth" for core business concepts (e.g., "What is a conversion?", "How is engagement calculated?").
* **Key Actions:**
    * **Joining:** Combines Bronze models (e.g., joining ad performance data with ad creative data).
    * **Unifying:** Unions data from different platforms (e.g., a future `int_ad_performance_unioned.sql` model) to solve the "code duplication" problem.
    * **Business Logic:** Calculates complex, reusable metrics (e.g., `silver_meta_platform_stats.sql`).

### 4. `models/4.gold` (Data Marts & Aggregation)

This is our **data mart layer**, built for specific business end-users. These are our final, production-ready tables.

* **Purpose:** To provide aggregated, analytics-ready tables for consumption by BI tools, ML models, or analysts.
* **Key Actions:**
    * **Aggregation:** Models are often aggregated by key dimensions (e.g., `campaign`, `date`, `platform`).
    * **Final Joins:** Joins Silver models with common dimensions (like `dim_calendar`).
    * **BI-Ready:** The `gold_marketing_campaign_stats.sql` model is the "analytics-friendly flat table" required by the brief, ready for direct querying.

### 🎯 Project Scope & Demonstration

This project serves as a **proof of concept** for the Bronze-Silver-Gold architecture.

To demonstrate the pattern, the data pipelines for **Meta (Facebook)** and **TikTok** have been built out (Bronze, Silver) and integrated into the final `gold_marketing_campaign_stats` model.

Given the time constraints of the exercise, the pipelines for the other platforms (Google Ads, DV360, SA360, GA4) have not been built. However, they would follow the **exact same modular pattern**:

1.  A new `bronze` model would be created for each source table.
2.  New `silver` models would be built to conform their data.
3.  The final, conformed Silver models would be **unioned** in an intermediate model (e.g., `int_all_platforms_unioned`) before being fed into the `gold` layer.

This approach ensures that as new platforms are added, we only need to add new modular components without ever refactoring the core logic.

### Data Flow Diagram

The data flows in one direction, ensuring a Directed Acyclic Graph (DAG) and making the project easy to debug and maintain.

> `Raw Data` (Snowflake) → `1.sources` (Source Tests) → `2.bronze` (Staging) → `3.silver` (Intermediate) → `4.gold` (Marts)

---

## 🚀 How to Run This Project

### Initial Setup

1.  **Install Packages:** Run `dbt deps` to install required packages like `dbt_utils` and `dbt_expectations`.
2.  **Load Mock Data:** Run `dbt seed` to load the mock CSV data from the `seeds/` directory into your target database. This is required for the models to run.

### Core Commands

* **Build & Test Everything:**
    ```bash
    dbt build
    ```

* **Run Only Final Models:**
    ```bash
    # This will run all upstream models and tests for the final mart
    dbt build --select gold_marketing_campaign_stats
    ```

* **Run Models:**
    ```bash
    dbt run
    ```

* **Run Tests:**
    ```bash
    dbt test
    ```

* **Generate Documentation:**
    ```bash
    dbt docs generate
    ```
    Then, click the "View Documentation" book icon in dbt Cloud to see the full project documentation and lineage graph.
