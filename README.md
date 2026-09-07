# Databricks E-Commerce Pipeline Demo

An end-to-end data engineering pipeline built on Databricks using Delta Live Tables (DLT) for processing e-commerce data. The project follows the Medallion Architecture to progressively transform raw data files into analytics-ready dimensional models.

## Architecture

This project organizes data processing into four distinct layers (pipelines) defined via Databricks Asset Bundles (`databricks/databricks.yml`):

1. **Raw (Bronze Layer)**
   - **Path**: `src/raw/`
   - **Purpose**: Ingests raw data files from a storage volume as they arrive.
   - **Key Components**: `generic_ingestion.py` handles the landing of raw events and datasets into raw Delta tables.

2. **Staging (Silver Layer - Cleansing)**
   - **Path**: `src/staging/`
   - **Purpose**: Cleanses, standardizes, and validates the raw data into typed tables.
   - **Key Entities**: `customer`, `order`, `order_item`, `product`, `returns`, `return_item`.

3. **Intermediate (Silver Layer - Business Logic)**
   - **Path**: `src/intermediate/`
   - **Purpose**: Applies complex business logic, joins normalized staging tables, and prepares the data for dimensional modeling.
   - **Key Entities**: `addresses`, `order_items`, `return_items`.

4. **Analytics (Gold Layer - Dimensional Modeling)**
   - **Path**: `src/analytics/`
   - **Purpose**: Creates highly optimized Dimension and Fact tables for BI, reporting, and downstream analytics.
   - **Key Features**: Implements Slowly Changing Dimensions (SCD Type 2) using DLT's `apply_changes()` to maintain history (e.g., `customer_dim`, `customer_contact_dim`).

## Project Structure

```text
.
├── README.md
└── databricks/
    ├── databricks.yml               # Databricks Asset Bundle (DAB) config (Pipelines & Jobs)
    └── src/
        ├── raw/                     # Raw ingestion logic
        ├── staging/                 # Staging tables (DLT) & schemas
        ├── intermediate/            # Intermediate transformations
        └── analytics/               # Analytics/Gold layer (Dims & Facts)
```

## Key Technologies Used

- **Databricks Asset Bundles (DABs)**: Used to define the infrastructure as code (IaC), managing the deployment of pipelines and workflows.
- **Delta Live Tables (DLT)**: Declarative framework to build reliable, maintainable, and testable data processing pipelines.
- **Auto Loader & File Arrival Triggers**: The main workflow (`trigger_raw_ingestion`) is configured to automatically trigger upon file arrival in the configured Databricks Volume.
- **SCD Type 2**: Advanced state management in DLT to track historical changes of dimensions over time.

## Deployment & Usage

### Prerequisites
- Install the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html).
- Have an active Databricks workspace with Unity Catalog enabled.

### Steps
1. **Authenticate to Databricks:**
   ```bash
   databricks auth login --host <your-workspace-url>
   ```

2. **Deploy the Bundle:**
   Navigate to the `databricks/` directory and deploy the bundle to your environment.
   ```bash
   cd databricks
   databricks bundle deploy -t <target-environment>
   ```

3. **Run the Pipeline:**
   You can manually trigger the workflow job defined in the bundle:
   ```bash
   databricks bundle run trigger_raw_ingestion
   ```
   *Note: The job is also configured to run automatically upon file arrival in the raw volume (`/Volumes/ecomm/raw/raw_vol/`).*