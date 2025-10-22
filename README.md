# ACME Supermarkets Data Pipeline

A comprehensive Databricks Delta Live Tables (DLT) pipeline project built with [LakehousePlumber](https://github.com/mmodarre/lakehouse_plumber), implementing a modern medallion architecture for retail data analytics.

## 📋 Overview

This project implements an enterprise-grade data pipeline for ACME Supermarkets, integrating data from multiple source systems:

- **NCR** - Point of Sale (POS) transactions and terminal data
- **SAP** - Enterprise Resource Planning (ERP) data including inventory, purchasing, and warehouse operations
- **SFCC** (Salesforce Commerce Cloud) - E-commerce platform data including online orders and customer information

The pipeline follows the **medallion architecture** pattern (Raw → Bronze → Silver) to progressively refine data quality and structure:

1. **Raw Layer** - Ingests raw data files from landing zones with minimal transformation
2. **Bronze Layer** - Cleanses and standardizes data, adds operational metadata
3. **Silver Layer** - Creates analytics-ready dimensions and facts with:
   - **Dimensions** (SCD Type 2) - Customer, product, location, supplier, etc.
   - **Facts** - Transactional data including POS transactions, sales orders, inventory movements, etc.

## 🏗️ Project Architecture

```
acme_supermarkets_lhp/
├── pipelines/              # Pipeline configurations by layer
│   ├── 01_raw_ingestion/   # Raw data ingestion configs
│   │   ├── NCR/            # 7 tables: POS, terminals, locations, etc.
│   │   ├── SAP/            # 17 tables: ERP, inventory, purchasing, etc.
│   │   └── SFCC/           # 7 tables: E-commerce orders, customers, etc.
│   ├── 02_bronze/          # Bronze layer transformations
│   │   ├── NCR/, SAP/, SFCC/
│   └── 03_silver/          # Analytics-ready data
│       ├── dimensions/     # 15 dimension tables (SCD2)
│       └── facts/          # 16 fact tables
│
├── templates/              # Reusable LHP templates
│   ├── TMPL001_csv_ingestion_template.yaml
│   ├── TMPL002_json_text_ingestion_template.yaml
│   ├── TMPL003_parquet_ingestion_template.yaml
│   ├── TMPL004_raw_to_bronze_standard_template.yaml
│   ├── TMPL005_silver_dimension_scd2_template.yaml
│   ├── TMPL006_silver_fact_scd2_template.yaml
│   ├── TMPL007_silver_accumulating_fact_template.yaml
│   ├── TMPL008_silver_snapshot_dimension_scd2_template.yaml
│   └── TMPL009_silver_transactional_fact_template.yaml
│
├── schemas/                # Table schema definitions
│   ├── customer_schema.yaml, product.yaml, etc.
│   └── (29 schema files covering all entities)
│
├── substitutions/          # Environment-specific configurations
│   ├── dev.yaml            # Development environment
│   ├── tst.yaml            # Test environment
│   └── prd.yaml            # Production environment
│
├── expectations/           # Data quality rules
│   ├── check_no_rescued_data.json
│   └── customer_quality.json.tmpl
│
├── resources/              # Databricks resources
│   ├── lhp/                # Generated DLT pipeline YAML files
│   └── acme_supermarkets_lhp_orchestration.job.yml
│
├── py_functions/           # Custom Python functions
│   ├── product_snapshot_func.py
│   └── timestamp_converter.py
│
├── generated/              # Generated DLT Python code (do not edit)
│   └── dev/                # Environment-specific generated code
│
├── docs/                   # Documentation
│   └── Database_schema.md  # Complete database schema reference
│
├── lhp.yaml                # LakehousePlumber project config
└── databricks.yml          # Databricks Asset Bundles config
```

## 📊 Data Model

The project implements a comprehensive retail data model covering:

- **Reference Data**: Brands, categories, payment methods, carriers, users
- **ERP**: Locations, products, suppliers, purchase orders, goods receipts, transfers
- **CRM**: Customers, addresses, payment methods
- **Inventory**: Store and warehouse inventory tracking with transaction history
- **POS**: Point of sale transactions, payments, terminals, promotions
- **E-commerce**: Online orders, shipments, fulfillment tracking
- **Loyalty**: Loyalty programs, accounts, and transactions
- **Financial**: Chart of accounts, journal entries

See `docs/Database_schema.md` for complete schema documentation.

## 🚀 Getting Started

### Prerequisites

- **Python 3.8+**
- **LakehousePlumber** (>= 0.5.0, < 0.7.0)
- **Databricks workspace** with Delta Live Tables enabled
- **Databricks CLI** configured

### Installation

1. **Install LakehousePlumber**:
   ```bash
   pip install lakehouse-plumber
   ```

2. **Clone this repository**:
   ```bash
   git clone <repository-url>
   cd acme_supermarkets_lhp
   ```

3. **Configure your environment**:
   - Edit `substitutions/dev.yaml` with your environment-specific values (catalogs, schemas, paths)
   - Update `databricks.yml` with your workspace details

### Quick Start

1. **Validate all pipeline configurations**:
   ```bash
   lhp validate --env dev
   ```

2. **Generate DLT pipeline code**:
   ```bash
   lhp generate --env dev
   ```

3. **Deploy to Databricks** (using Databricks Asset Bundles):
   ```bash
   databricks bundle deploy -t dev
   ```

4. **Run the orchestration job**:
   ```bash
   databricks bundle run acme_supermarkets_lhp_orchestration -t dev
   ```

## 🔧 Development Workflow

### Creating a New Pipeline

1. **Create a pipeline configuration directory**:
   ```bash
   mkdir -p pipelines/04_gold/aggregates
   ```

2. **Create a flowgroup YAML file**:
   ```bash
   touch pipelines/04_gold/aggregates/sales_summary.yaml
   ```

3. **Define your pipeline using templates**:
   ```yaml
   pipeline: gold_aggregates
   flowgroup: sales_summary
   
   use_template: materialized_view_template
   template_parameters:
     table_name: daily_sales_summary
     source_tables: 
       - fact_pos_transaction
       - dim_product
   ```

4. **Validate the configuration**:
   ```bash
   lhp validate --env dev
   ```

5. **Generate and preview** (dry-run mode):
   ```bash
   lhp generate --env dev --dry-run --verbose
   ```

6. **Generate the actual code**:
   ```bash
   lhp generate --env dev
   ```

### Working with Templates

**List available templates**:
```bash
lhp list-templates
```

**Show template details**:
```bash
lhp show-template TMPL005_silver_dimension_scd2_template
```

**Validate templates only**:
```bash
lhp validate --env dev --templates-only
```

### Working with Presets

**List available presets**:
```bash
lhp list-presets
```

**Show preset configuration**:
```bash
lhp show-preset bronze_layer
```

## 📚 Available Commands

| Command | Description |
|---------|-------------|
| `lhp validate --env <env>` | Validate all pipeline configurations for specified environment |
| `lhp generate --env <env>` | Generate DLT Python code from pipeline configurations |
| `lhp list-presets` | List all available reusable presets |
| `lhp list-templates` | List all available templates |
| `lhp show <flowgroup>` | Show resolved configuration for a specific flowgroup |
| `lhp show-template <name>` | Display template details |
| `lhp --version` | Show LakehousePlumber version |

### Additional Flags

- `--dry-run` - Preview changes without writing files
- `--verbose` - Enable detailed logging
- `--templates-only` - Validate/operate on templates only

## 🌍 Environments

The project supports multiple environments with environment-specific configurations in `substitutions/`:

- **dev** - Development environment for testing and iteration
- **tst** - Test environment for validation
- **prd** - Production environment

Each environment can have different:
- Catalog and schema names
- Storage paths and volumes
- Secret scopes and credentials
- API endpoints

## 🎯 Data Quality

Data quality expectations are defined in the `expectations/` directory:

- **check_no_rescued_data.json** - Ensures no data is rescued during ingestion
- **customer_quality.json.tmpl** - Customer data quality rules

Expectations are automatically applied during DLT pipeline execution.

## 📖 Project Metadata

- **Name**: acme_supermarkets_data_pipeline
- **Version**: 1.0
- **Author**: Mehdi Modarressi
- **Created**: 2025-10-15
- **Required LHP Version**: >= 0.5.0, < 0.7.0

## 🔗 Additional Resources

- [LakehousePlumber Documentation](https://github.com/mmodarre/lakehouse_plumber)
- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [Database Schema Documentation](docs/Database_schema.md)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)

## 🤝 Contributing

When contributing to this project:

1. Always validate configurations before committing: `lhp validate --env dev`
2. Never edit files in the `generated/` directory directly
3. Use templates for consistency and maintainability
4. Update schema files when data models change
5. Test changes in `dev` environment before promoting

## 📝 Notes

- **Generated code** in `generated/` directory is auto-generated - do not edit manually
- **Operational metadata** columns (`_source_file_path`, `_processing_timestamp`, `_source_file_name`) are automatically added to tables
- **Version control**: Only commit source configurations (pipelines, templates, schemas), not generated code
- **Templates vs Pipelines**: Templates are reusable patterns, pipelines are specific implementations

---

*Built with ❤️ using [LakehousePlumber](https://github.com/mmodarre/lakehouse_plumber)* 