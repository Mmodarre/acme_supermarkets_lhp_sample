# ACME Supermarkets - Semantic Layer Analysis
## Comprehensive Database Inspection & Recommendations

**Generated:** 2025-10-22 23:25:31  
**Catalog:** acme_supermarkets  
**Schema:** edw_silver  
**Total Tables:** 43 (15 Dimensions, 28 Facts)

---

## Executive Summary

This document provides a comprehensive analysis of the ACME Supermarkets silver layer and recommendations for building a semantic layer (Gold layer) to support business intelligence and analytics.

### Key Findings

✅ **Strong Foundation:**
- 15 well-structured dimensions with SCD Type 2 implementation
- 28 fact tables covering all business processes
- ~850K+ total rows of transactional data
- Clean separation of source systems (NCR POS, SAP ERP, SFCC E-commerce)

⚠️ **Critical Gaps:**
- No Date/Time dimension table
- No Sales Channel dimension  
- No unified sales fact combining POS + E-commerce
- Source system prefixes prevent easy conformation

### Data Volume Highlights

| Category | Volume |
|----------|--------|
| **POS Transactions** | 50,850 transactions, 153,147 line items |
| **E-commerce Orders** | 37,701 orders (snapshot), 120,592 line items |
| **Store Inventory Records** | 401,062 records |
| **Products** | 13,939 products |
| **Customers** | 160 active customers |
| **Locations** | 54 locations (stores + warehouses) |

---

## Section 1: Actual Silver Layer Inventory

### 1.1 Dimension Tables (15)

| Table Name | Rows | Columns | SCD Type | Source | Description |
|------------|------|---------|----------|--------|-------------|
| **dim_ncr_location** | 54 | 16 | Type 2 | NCR | Stores and warehouse locations |
| **dim_ncr_pmt_method** | 16 | 10 | Type 2 | NCR | Payment methods (CASH, CREDIT, etc) |
| **dim_ncr_pos_term** | 135 | 12 | Type 2 | NCR | POS terminals in stores |
| **dim_sap_brand** | 4,010 | 11 | Type 2 | SAP | Product brands |
| **dim_sap_carrier** | 6 | 10 | Type 2 | SAP | Shipping carriers |
| **dim_sap_cat** | 4,162 | 13 | Type 1 | SAP | Product categories (hierarchical) |
| **dim_sap_pmt_terms** | 3 | 10 | Type 1 | SAP | Payment terms (Net 30, Net 60) |
| **dim_sap_prd** | 13,939 | 25 | Type 1 | SAP | Product master (with snapshot history) |
| **dim_sap_sup** | 83 | 13 | Type 1 | SAP | Suppliers/vendors |
| **dim_sap_uom** | 13 | 10 | Type 1 | SAP | Units of measure |
| **dim_sap_user** | 1,322 | 16 | Type 1 | SAP | Employees/users |
| **dim_sap_whse** | 4 | 10 | Type 1 | SAP | Warehouses |
| **dim_sfcc_cust** | 160 | 17 | Type 1 | SFCC | Customers |
| **dim_sfcc_cust_addr** | 30,071 | 16 | Type 1 | SFCC | Customer addresses (includes history) |
| **dim_sfcc_reason_code** | 11 | 10 | Type 1 | SFCC | Reason codes |

**Key Observations:**
- All dimensions have surrogate keys (`*_key`) and natural keys (`*_id`)
- NCR dimensions use SCD Type 2 with `__START_AT` and `__END_AT` columns
- SAP/SFCC dimensions mostly Type 1 (current state only)
- Customer addresses surprisingly large (30K rows for 160 customers = historical tracking)

### 1.2 Fact Tables (28)

#### A. POS Sales Facts (NCR) - **~204K rows**

| Table Name | Rows | Grain | Type |
|------------|------|-------|------|
| **fct_ncr_pos_txn_hdr** | 50,850 | One row per transaction | Transactional |
| **fct_ncr_pos_txn_ln** | 153,147 | One row per line item | Transactional |
| **fct_ncr_pos_txn_pmt** | 50,850 | One row per transaction payment | Transactional |

**Measures Available:** total_gross, total_net, total_tax, total_discount, qty, unit_price, line_discount, line_total, amount

#### B. E-commerce Sales Facts (SFCC) - **~207K rows**

| Table Name | Rows | Grain | Type |
|------------|------|-------|------|
| **fct_sfcc_sls_ord_hdr** | 0 | Current state only | Current |
| **fct_sfcc_sls_ord_hdr_hist** | 48,909 | Order status changes | History |
| **fct_sfcc_sls_ord_hdr_snapshot** | 37,701 | Order milestones | Accumulating Snapshot |
| **fct_sfcc_sls_ord_ln** | 120,592 | One row per line item | Transactional |

**Pattern:** Three-table pattern for accumulating snapshot facts:
- `_hdr`: Current state (0 rows - gets overwritten)
- `_hdr_hist`: Full history of all status changes
- `_hdr_snapshot`: Snapshot of current milestone dates

**Measures Available:** subtotal, tax_amount, shipping_amount, discount_amount, total_amount, qty, unit_price, line_total, allocated_qty, shipped_qty, cancelled_qty

#### C. E-commerce Fulfillment Facts (SFCC) - **~66K rows**

| Table Name | Rows | Grain | Type |
|------------|------|-------|------|
| **fct_sfcc_shmpt_hdr_hist** | 19,730 | Shipment status changes | History |
| **fct_sfcc_shmpt_hdr_snapshot** | 11,298 | Shipment milestones | Accumulating Snapshot |
| **fct_sfcc_shmpt_ln** | 34,766 | One row per shipped line | Transactional |

#### D. Inventory Facts (NCR) - **~401K rows**

| Table Name | Rows | Grain | Type |
|------------|------|-------|------|
| **fct_ncr_str_invtry** | 401,062 | One row per product per store (SCD Type 2) | Periodic Snapshot |

**Measures Available:** on_hand_qty, reserved_qty, safety_stock_qty, reorder_point

#### E. Supply Chain Facts (SAP) - **~291 rows**

| Table Name | Rows | Grain | Type |
|------------|------|-------|------|
| **fct_sap_prch_ord_hdr_hist** | 7 | Purchase order status changes | History |
| **fct_sap_prch_ord_hdr_snapshot** | 5 | PO milestones | Accumulating Snapshot |
| **fct_sap_prch_ord_ln** | 140 | One row per PO line | Transactional |
| **fct_sap_rcpt_hdr_hist** | 5 | Receipt status changes | History |
| **fct_sap_rcpt_hdr_snapshot** | 5 | Receipt milestones | Accumulating Snapshot |
| **fct_sap_rcpt_ln** | 100 | One row per receipt line | Transactional |
| **fct_sap_xfer_ord_hdr_hist** | 5 | Transfer order status changes | History |
| **fct_sap_xfer_ord_hdr_snapshot** | 3 | Transfer milestones | Accumulating Snapshot |
| **fct_sap_xfer_ord_ln** | 17 | One row per transfer line | Transactional |
| **fct_sap_whse_shpmt_hdr_hist** | 3 | Warehouse shipment changes | History |
| **fct_sap_whse_shpmt_hdr_snapshot** | 3 | Shipment milestones | Accumulating Snapshot |
| **fct_sap_whse_shpmt_ln** | 14 | One row per shipment line | Transactional |

**Key Pattern:** Accumulating snapshot implementation uses dual tables (_hist + _snapshot) to track process milestones

---

## Section 2: Gap Analysis - What's Missing

### 2.1 Critical Missing Components

#### ⚠️ **PRIORITY 1: Date Dimension (CRITICAL)**

**Impact:** Cannot perform time-series analysis without a proper date dimension.

**Required Attributes:**
```sql
dim_date
├── date_key (INT surrogate: YYYYMMDD)
├── full_date (DATE)
├── Calendar hierarchy
│   ├── day_of_week (1-7), day_name (Monday-Sunday)
│   ├── day_of_month (1-31), day_of_year (1-366)
│   ├── week_of_year, iso_week
│   ├── month_num (1-12), month_name, month_abbr
│   ├── quarter (1-4), quarter_name (Q1, Q2, etc)
│   ├── year (YYYY)
├── Fiscal hierarchy (if different from calendar)
│   ├── fiscal_year, fiscal_quarter, fiscal_month
│   ├── fiscal_week
├── Business flags
│   ├── is_weekend (BOOLEAN)
│   ├── is_holiday (BOOLEAN)
│   ├── holiday_name (STRING)
│   ├── is_business_day (BOOLEAN)
└── Relative periods
    ├── days_from_today, weeks_from_today
    ├── is_current_month, is_last_month
    ├── is_current_quarter, is_last_quarter
```

**Date Range:** 2020-01-01 to 2030-12-31 (10+ years)

#### ⚠️ **PRIORITY 2: Sales Channel Dimension**

**Impact:** Cannot distinguish POS vs E-commerce sales in unified views.

```sql
dim_sales_channel
├── channel_key (INT surrogate)
├── channel_id (STRING natural key)
├── channel_name (In-Store POS, E-Commerce Website, Mobile App)
├── channel_type (Physical, Digital)
├── channel_category (Retail, Online)
└── source_system (NCR, SFCC)
```

**Sample Data:**
- 1 | "POS" | "In-Store POS" | "Physical" | "Retail" | "NCR"
- 2 | "ECOM" | "E-Commerce Website" | "Digital" | "Online" | "SFCC"
- 3 | "MOBILE" | "Mobile App" | "Digital" | "Online" | "SFCC"

#### ⚠️ **PRIORITY 3: Unified Sales Fact Table**

**Impact:** Currently POS and E-commerce sales are in separate tables requiring UNIONs.

**Current State:**
- POS Sales: `fct_ncr_pos_txn_ln` (153K rows)
- E-commerce Sales: `fct_sfcc_sls_ord_ln` (121K rows)

**Proposed:** `fact_sales_unified` (274K rows)

```sql
fact_sales_unified
├── Foreign Keys
│   ├── date_key → dim_date
│   ├── product_key → dim_product (conformed)
│   ├── customer_key → dim_customer (conformed)
│   ├── location_key → dim_location (store or warehouse)
│   ├── sales_channel_key → dim_sales_channel
│   ├── payment_method_key → dim_payment_method
│   └── employee_key → dim_employee (cashier/processor)
├── Degenerate Dimensions
│   ├── transaction_id / order_id (STRING)
│   ├── transaction_number / order_number
│   └── line_number
├── Measures
│   ├── quantity_sold (INT)
│   ├── unit_price (DECIMAL)
│   ├── line_discount_amount (DECIMAL)
│   ├── line_subtotal (DECIMAL)
│   ├── line_tax_amount (DECIMAL)
│   ├── line_total_amount (DECIMAL)
│   └── unit_cost (DECIMAL - for margin analysis)
└── Flags
    ├── is_returned (BOOLEAN)
    ├── is_cancelled (BOOLEAN)
    └── fulfillment_status (for e-commerce lines)
```

### 2.2 Dimension Conformation Issues

**Current Problem:** Dimensions use source system prefixes preventing direct joins.

| Entity | Current Tables | Issue |
|--------|----------------|-------|
| Location | `dim_ncr_location` | Should be `dim_location` (conformed across all sources) |
| Payment Method | `dim_ncr_pmt_method` | Should be `dim_payment_method` |
| Product | `dim_sap_prd` | Should be `dim_product` (already mostly conformed) |
| Customer | `dim_sfcc_cust` | Should be `dim_customer` |
| User/Employee | `dim_sap_user` | Should be `dim_employee` |
| Brand | `dim_sap_brand` | Should be `dim_brand` |
| Category | `dim_sap_cat` | Should be `dim_category` |
| Supplier | `dim_sap_sup` | Should be `dim_supplier` |
| Carrier | `dim_sap_carrier` | Should be `dim_carrier` |

**Recommendation:** Create conformed dimensions in semantic layer with business-friendly names.

---

## Section 3: Semantic Layer Design - Dimensions

### 3.1 Conformed Dimension Design

#### **dim_date** (NEW - To be created)
See Section 2.1 for full structure.

#### **dim_product** (Conformed from dim_sap_prd)

```sql
CREATE OR REPLACE VIEW gold.dim_product AS
SELECT
    prd_key AS product_key,
    product_id,
    sku,
    upc,
    name AS product_name,
    -- Brand attributes (denormalized)
    b.brand_id,
    b.name AS brand_name,
    -- Category attributes (denormalized with hierarchy)
    c.category_id,
    c.name AS category_name,
    c.parent_category_id,
    c.level AS category_level,
    c.path AS category_path,
    -- UOM attributes
    u.uom_id,
    u.name AS uom_name,
    u.code AS uom_code,
    -- Product attributes
    packaging,
    quantity,
    base_cost,
    base_price,
    ROUND(base_price - base_cost, 2) AS unit_margin,
    ROUND((base_price - base_cost) / NULLIF(base_price, 0) * 100, 2) AS margin_pct,
    reorder_quantity,
    shelf_life_days,
    nutrition_data,
    status AS product_status,
    -- Audit columns
    created_at AS product_created_at,
    updated_at AS product_updated_at,
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM acme_supermarkets.edw_silver.dim_sap_prd p
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_brand b ON p.brand_id = b.brand_id AND b.__END_AT IS NULL
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_cat c ON p.category_id = c.category_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_uom u ON p.uom_id = u.uom_id;
```

**Enhancements:**
- Denormalized brand and category for easy querying
- Calculated margin measures
- Clear SCD Type 2 flags

#### **dim_customer** (Conformed from dim_sfcc_cust)

```sql
CREATE OR REPLACE VIEW gold.dim_customer AS
SELECT
    cust_key AS customer_key,
    customer_id,
    external_ref,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    phone,
    dob AS date_of_birth,
    YEAR(CURRENT_DATE()) - YEAR(dob) AS age,
    CASE 
        WHEN YEAR(CURRENT_DATE()) - YEAR(dob) < 25 THEN '18-24'
        WHEN YEAR(CURRENT_DATE()) - YEAR(dob) < 35 THEN '25-34'
        WHEN YEAR(CURRENT_DATE()) - YEAR(dob) < 45 THEN '35-44'
        WHEN YEAR(CURRENT_DATE()) - YEAR(dob) < 55 THEN '45-54'
        WHEN YEAR(CURRENT_DATE()) - YEAR(dob) < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    segment AS customer_segment,
    status AS customer_status,
    created_at AS customer_since_date,
    DATEDIFF(CURRENT_DATE(), created_at) AS customer_tenure_days,
    FLOOR(DATEDIFF(CURRENT_DATE(), created_at) / 30) AS customer_tenure_months,
    -- Default address (denormalized)
    a.address_id AS default_address_id,
    CONCAT_WS(', ', a.line1, a.city, a.state, a.postcode) AS default_address,
    a.city,
    a.state,
    a.postcode,
    a.country,
    -- SCD
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM acme_supermarkets.edw_silver.dim_sfcc_cust c
LEFT JOIN acme_supermarkets.edw_silver.dim_sfcc_cust_addr a 
    ON c.customer_id = a.customer_id 
    AND a.is_default_shipping = TRUE
    AND a.__END_AT IS NULL;
```

**Enhancements:**
- Calculated age and age groups
- Customer tenure metrics
- Denormalized default address

#### **dim_location** (Conformed from dim_ncr_location)

```sql
CREATE OR REPLACE VIEW gold.dim_location AS
SELECT
    location_key,
    location_id,
    location_type,
    name AS location_name,
    address_line1,
    address_line2,
    city,
    state,
    zip_code,
    country,
    -- Derived attributes
    CASE 
        WHEN state IN ('CA', 'OR', 'WA') THEN 'West'
        WHEN state IN ('TX', 'AZ', 'NM') THEN 'Southwest'
        WHEN state IN ('NY', 'NJ', 'PA', 'MA') THEN 'Northeast'
        WHEN state IN ('FL', 'GA', 'NC', 'SC') THEN 'Southeast'
        ELSE 'Other'
    END AS region,
    status AS location_status,
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM acme_supermarkets.edw_silver.dim_ncr_location;
```

#### **dim_sales_channel** (NEW - To be created)

```sql
CREATE OR REPLACE TABLE gold.dim_sales_channel (
    channel_key INT PRIMARY KEY,
    channel_id STRING,
    channel_name STRING,
    channel_type STRING,
    channel_category STRING,
    source_system STRING
);

INSERT INTO gold.dim_sales_channel VALUES
(1, 'POS', 'In-Store Point of Sale', 'Physical', 'Retail', 'NCR'),
(2, 'ECOM', 'E-Commerce Website', 'Digital', 'Online', 'SFCC'),
(3, 'MOBILE', 'Mobile Application', 'Digital', 'Online', 'SFCC');
```

#### **dim_employee** (Conformed from dim_sap_user)

```sql
CREATE OR REPLACE VIEW gold.dim_employee AS
SELECT
    user_key AS employee_key,
    user_id AS employee_id,
    username,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    role AS employee_role,
    CASE 
        WHEN role = 'manager' THEN 'Management'
        WHEN role = 'cashier' THEN 'Sales'
        WHEN role = 'picker' THEN 'Fulfillment'
        ELSE 'Other'
    END AS role_category,
    -- Store assignment
    store_id AS assigned_location_id,
    l.name AS assigned_location_name,
    status AS employee_status,
    created_at AS hired_date,
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM acme_supermarkets.edw_silver.dim_sap_user u
LEFT JOIN acme_supermarkets.edw_silver.dim_ncr_location l 
    ON u.store_id = l.location_id AND l.__END_AT IS NULL;
```

### 3.2 Additional Conformed Dimensions

Create similar views for:
- `dim_payment_method` (from `dim_ncr_pmt_method`)
- `dim_brand` (from `dim_sap_brand`)
- `dim_category` (from `dim_sap_cat`)
- `dim_supplier` (from `dim_sap_sup`)
- `dim_carrier` (from `dim_sap_carrier`)

---

## Section 4: Semantic Layer Design - Fact Tables & Measures

### 4.1 Unified Sales Fact Table

#### **fact_sales_unified** - Combining POS + E-commerce

```sql
CREATE OR REPLACE VIEW gold.fact_sales_unified AS

-- POS Sales (In-Store)
SELECT
    -- Date key (requires dim_date table)
    TO_NUMBER(DATE_FORMAT(h.txn_datetime, 'yyyyMMdd')) AS date_key,
    h.txn_datetime AS transaction_datetime,
    
    -- Dimension keys
    1 AS sales_channel_key, -- POS
    h.store_id AS location_key, -- Join to dim_location
    l.product_id AS product_key, -- Join to dim_product  
    h.customer_id AS customer_key, -- Join to dim_customer
    p.method_id AS payment_method_key, -- Join to dim_payment_method
    h.cashier_user_id AS employee_key, -- Join to dim_employee
    NULL AS carrier_key,
    
    -- Degenerate dimensions
    CAST(h.txn_id AS STRING) AS transaction_number,
    l.line_number,
    'POS' AS source_system,
    
    -- Measures
    l.qty AS quantity_sold,
    l.unit_price,
    l.line_discount AS discount_amount,
    l.line_total - l.line_discount AS subtotal_amount,
    ROUND((l.line_total - l.line_discount) * (h.total_tax / NULLIF(h.total_net, 0)), 2) AS tax_amount,
    l.line_total AS total_amount,
    
    -- Cost and margin (from product dimension)
    prd.base_cost AS unit_cost,
    l.qty * prd.base_cost AS total_cost,
    l.line_total - (l.qty * prd.base_cost) AS gross_profit,
    ROUND((l.line_total - (l.qty * prd.base_cost)) / NULLIF(l.line_total, 0) * 100, 2) AS margin_pct,
    
    -- Flags
    FALSE AS is_returned,
    FALSE AS is_cancelled,
    'completed' AS fulfillment_status
    
FROM acme_supermarkets.edw_silver.fct_ncr_pos_txn_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_ncr_pos_txn_hdr h ON l.txn_id = h.txn_id
INNER JOIN acme_supermarkets.edw_silver.fct_ncr_pos_txn_pmt p ON h.txn_id = p.txn_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd prd ON l.product_id = prd.product_id AND prd.__END_AT IS NULL

UNION ALL

-- E-commerce Sales (Online)
SELECT
    -- Date key
    TO_NUMBER(DATE_FORMAT(h.order_date, 'yyyyMMdd')) AS date_key,
    h.order_date AS transaction_datetime,
    
    -- Dimension keys
    2 AS sales_channel_key, -- E-commerce
    h.warehouse_id AS location_key, -- Fulfillment warehouse
    l.product_id AS product_key,
    h.customer_id AS customer_key,
    h.payment_method_id AS payment_method_key,
    NULL AS employee_key,
    h.carrier_id AS carrier_key,
    
    -- Degenerate dimensions
    h.order_number AS transaction_number,
    l.line_number,
    'SFCC' AS source_system,
    
    -- Measures
    l.qty AS quantity_sold,
    l.unit_price,
    l.discount_amount,
    l.line_total - l.discount_amount - l.tax_amount AS subtotal_amount,
    l.tax_amount,
    l.line_total AS total_amount,
    
    -- Cost and margin
    prd.base_cost AS unit_cost,
    l.qty * prd.base_cost AS total_cost,
    l.line_total - (l.qty * prd.base_cost) AS gross_profit,
    ROUND((l.line_total - (l.qty * prd.base_cost)) / NULLIF(l.line_total, 0) * 100, 2) AS margin_pct,
    
    -- Flags
    FALSE AS is_returned,
    CASE WHEN l.cancelled_qty > 0 THEN TRUE ELSE FALSE END AS is_cancelled,
    l.status AS fulfillment_status
    
FROM acme_supermarkets.edw_silver.fct_sfcc_sls_ord_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sfcc_sls_ord_hdr_hist h ON l.order_id = h.order_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd prd ON l.product_id = prd.product_id AND prd.__END_AT IS NULL;
```

**Key Measures Available:**
- **Quantity Metrics:** quantity_sold
- **Revenue Metrics:** subtotal_amount, discount_amount, tax_amount, total_amount
- **Cost Metrics:** unit_cost, total_cost
- **Profitability Metrics:** gross_profit, margin_pct

### 4.2 Inventory Snapshot Fact

```sql
CREATE OR REPLACE VIEW gold.fact_inventory_snapshot AS
SELECT
    -- Date key
    TO_NUMBER(DATE_FORMAT(last_update_dttm, 'yyyyMMdd')) AS date_key,
    last_update_dttm AS snapshot_datetime,
    
    -- Dimension keys
    store_id AS location_key,
    product_id AS product_key,
    
    -- Stock level measures
    on_hand_qty AS on_hand_quantity,
    reserved_qty AS reserved_quantity,
    on_hand_qty - reserved_qty AS available_quantity,
    safety_stock_qty AS safety_stock_quantity,
    reorder_point AS reorder_point_quantity,
    
    -- Calculated measures
    CASE 
        WHEN on_hand_qty - reserved_qty <= 0 THEN TRUE 
        ELSE FALSE 
    END AS is_out_of_stock,
    
    CASE 
        WHEN on_hand_qty - reserved_qty < safety_stock_qty THEN TRUE 
        ELSE FALSE 
    END AS is_below_safety_stock,
    
    CASE 
        WHEN on_hand_qty - reserved_qty <= reorder_point THEN TRUE 
        ELSE FALSE 
    END AS needs_reorder,
    
    -- Inventory value (requires product base_cost)
    p.base_cost * on_hand_qty AS inventory_value_at_cost,
    p.base_price * on_hand_qty AS inventory_value_at_retail,
    (p.base_price - p.base_cost) * on_hand_qty AS potential_profit,
    
    -- SCD tracking
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.fct_ncr_str_invtry i
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd p 
    ON i.product_id = p.product_id AND p.__END_AT IS NULL;
```

**Key Measures:**
- **Stock Levels:** on_hand, reserved, available
- **Threshold Metrics:** safety_stock, reorder_point
- **Value Metrics:** inventory_value_at_cost, inventory_value_at_retail, potential_profit
- **Flags:** is_out_of_stock, is_below_safety_stock, needs_reorder

### 4.3 Order Fulfillment Fact (Accumulating Snapshot)

```sql
CREATE OR REPLACE VIEW gold.fact_order_fulfillment AS
SELECT
    -- Order identification
    s.order_id,
    h.order_number,
    
    -- Dimension keys (using date keys for each milestone)
    TO_NUMBER(DATE_FORMAT(h.order_date, 'yyyyMMdd')) AS order_date_key,
    TO_NUMBER(DATE_FORMAT(s.allocated_date, 'yyyyMMdd')) AS allocated_date_key,
    TO_NUMBER(DATE_FORMAT(s.shipped_date, 'yyyyMMdd')) AS shipped_date_key,
    TO_NUMBER(DATE_FORMAT(s.completed_date, 'yyyyMMdd')) AS delivered_date_key,
    
    h.customer_id AS customer_key,
    h.warehouse_id AS location_key,
    h.carrier_id AS carrier_key,
    
    -- Milestone timestamps
    h.order_date AS order_datetime,
    s.allocated_date AS allocated_datetime,
    s.shipped_date AS shipped_datetime,
    s.completed_date AS delivered_datetime,
    h.estimated_delivery_date,
    h.actual_delivery_date,
    
    -- Status
    s.current_status AS fulfillment_status,
    h.fulfillment_status AS order_fulfillment_status,
    
    -- Measures
    h.subtotal,
    h.tax_amount,
    h.shipping_amount,
    h.discount_amount,
    h.total_amount,
    
    -- Timing metrics (in hours/days)
    TIMESTAMPDIFF(HOUR, h.order_date, s.allocated_date) AS order_to_allocation_hours,
    TIMESTAMPDIFF(HOUR, s.allocated_date, s.shipped_date) AS allocation_to_ship_hours,
    TIMESTAMPDIFF(DAY, s.shipped_date, s.completed_date) AS ship_to_delivery_days,
    TIMESTAMPDIFF(DAY, h.order_date, s.completed_date) AS total_fulfillment_days,
    
    -- Performance flags
    CASE 
        WHEN s.completed_date IS NOT NULL 
        AND s.completed_date <= h.estimated_delivery_date 
        THEN TRUE 
        ELSE FALSE 
    END AS is_on_time_delivery,
    
    CASE 
        WHEN s.completed_date IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_completed
    
FROM acme_supermarkets.edw_silver.fct_sfcc_sls_ord_hdr_snapshot s
INNER JOIN acme_supermarkets.edw_silver.fct_sfcc_sls_ord_hdr_hist h 
    ON s.order_id = h.order_id;
```

**Key Measures:**
- **Order Value:** subtotal, tax, shipping, discount, total
- **Timing Metrics:** order_to_allocation_hours, allocation_to_ship_hours, ship_to_delivery_days, total_fulfillment_days
- **Performance Metrics:** is_on_time_delivery, is_completed

### 4.4 Purchase Orders Fact

```sql
CREATE OR REPLACE VIEW gold.fact_purchase_orders AS
SELECT
    -- Date keys
    TO_NUMBER(DATE_FORMAT(h.order_date, 'yyyyMMdd')) AS order_date_key,
    TO_NUMBER(DATE_FORMAT(h.expected_date, 'yyyyMMdd')) AS expected_date_key,
    TO_NUMBER(DATE_FORMAT(s.received_date, 'yyyyMMdd')) AS received_date_key,
    
    -- Dimension keys
    h.po_id,
    h.vendor_id AS supplier_key,
    h.ship_to_location_id AS location_key,
    l.product_id AS product_key,
    l.uom_id AS uom_key,
    l.po_line_id,
    
    -- Measures
    l.ordered_qty AS ordered_quantity,
    r.received_qty AS received_quantity,
    r.rejected_qty AS rejected_quantity,
    l.ordered_qty - COALESCE(r.received_qty, 0) AS backorder_quantity,
    
    l.unit_price,
    l.tax_rate,
    l.ordered_qty * l.unit_price AS line_amount,
    ROUND(l.ordered_qty * l.unit_price * l.tax_rate / 100, 2) AS line_tax_amount,
    ROUND(l.ordered_qty * l.unit_price * (1 + l.tax_rate / 100), 2) AS line_total,
    
    -- Status
    s.current_status AS po_status,
    
    -- Timing
    DATEDIFF(s.received_date, h.order_date) AS lead_time_days,
    DATEDIFF(s.received_date, h.expected_date) AS days_late_early,
    
    -- Performance flags
    CASE 
        WHEN s.received_date IS NOT NULL 
        AND s.received_date <= h.expected_date 
        THEN TRUE 
        ELSE FALSE 
    END AS is_on_time,
    
    COALESCE(r.received_qty, 0) / NULLIF(l.ordered_qty, 0) AS fill_rate
    
FROM acme_supermarkets.edw_silver.fct_sap_prch_ord_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sap_prch_ord_hdr_hist h ON l.po_id = h.po_id
LEFT JOIN acme_supermarkets.edw_silver.fct_sap_prch_ord_hdr_snapshot s ON l.po_id = s.po_id
LEFT JOIN acme_supermarkets.edw_silver.fct_sap_rcpt_ln r 
    ON l.po_line_id = r.po_line_id;
```

---

## Section 5: Business Metrics & KPIs

### 5.1 Sales & Revenue Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Total Revenue** | SUM(total_amount) | Overall sales performance |
| **Gross Sales** | SUM(subtotal_amount + discount_amount) | Sales before discounts |
| **Net Sales** | SUM(subtotal_amount) | Sales after discounts |
| **Average Transaction Value** | SUM(total_amount) / COUNT(DISTINCT transaction_number) | Basket size |
| **Units per Transaction** | SUM(quantity_sold) / COUNT(DISTINCT transaction_number) | Basket depth |
| **Discount Rate** | SUM(discount_amount) / SUM(gross_sales) | Promotion effectiveness |
| **Revenue per Customer** | SUM(total_amount) / COUNT(DISTINCT customer_key) | Customer value |

### 5.2 Profitability Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Gross Profit** | SUM(total_amount - total_cost) | Profit before expenses |
| **Gross Margin %** | (SUM(gross_profit) / SUM(total_amount)) * 100 | Profitability rate |
| **COGS** | SUM(total_cost) | Cost of goods sold |
| **Margin per Unit** | SUM(total_amount - total_cost) / SUM(quantity_sold) | Unit economics |

### 5.3 Inventory Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Inventory Value** | SUM(inventory_value_at_cost) | Working capital |
| **Stock Availability %** | (Products in stock / Total products) * 100 | Service level |
| **Stockout Rate** | COUNT(CASE WHEN is_out_of_stock) / COUNT(*) | Lost sales risk |
| **Days of Supply** | on_hand_qty / AVG(daily_sales_qty) | Inventory health |
| **Inventory Turnover** | COGS / AVG(inventory_value) | Efficiency |

### 5.4 Customer Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Unique Customers** | COUNT(DISTINCT customer_key) | Customer base size |
| **New Customers** | COUNT(DISTINCT CASE WHEN first_purchase_date = current_date...) | Acquisition |
| **Repeat Purchase Rate** | Customers with >1 purchase / Total customers | Retention |
| **Customer Lifetime Value** | SUM(total_amount) / COUNT(DISTINCT customer_key) * Avg customer lifespan | Long-term value |
| **Average Order Value** | SUM(total_amount) / COUNT(DISTINCT order_id) | Per-order spending |

### 5.5 Fulfillment & Operations Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Order Fill Rate** | Fulfilled qty / Ordered qty | Service level |
| **On-Time Delivery Rate** | On-time orders / Total orders | Reliability |
| **Average Fulfillment Time** | AVG(total_fulfillment_days) | Speed |
| **Perfect Order Rate** | (On-time + Complete + Undamaged) / Total | Quality |
| **Average Lead Time** | AVG(lead_time_days) | Supplier performance |

### 5.6 Channel Performance Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| **Revenue by Channel** | SUM(total_amount) GROUP BY sales_channel | Channel comparison |
| **Channel Mix %** | Channel revenue / Total revenue | Portfolio composition |
| **Channel Growth** | ((Current - Prior) / Prior) * 100 | Trend analysis |
| **Cross-Channel Customers** | Customers shopping both POS + Online | Omnichannel engagement |

---

## Section 6: Implementation Recommendations

### 6.1 Phased Approach

#### **Phase 1: Foundation (Week 1-2)**
1. Create `dim_date` table (2020-2030)
2. Create `dim_sales_channel` table
3. Create conformed dimension views:
   - `dim_product`
   - `dim_customer`
   - `dim_location`
   - `dim_employee`
   - `dim_payment_method`

#### **Phase 2: Core Analytics (Week 3-4)**
4. Create `fact_sales_unified` view
5. Create `fact_inventory_snapshot` view
6. Validate data quality and reconciliation
7. Build sample dashboards for testing

#### **Phase 3: Advanced Analytics (Week 5-6)**
8. Create `fact_order_fulfillment` view
9. Create `fact_purchase_orders` view
10. Implement pre-aggregated metrics tables
11. Add calculated measures and KPIs

#### **Phase 4: Optimization & Rollout (Week 7-8)**
12. Materialize frequently-used views as tables
13. Add indexes and optimize query performance
14. Document all metrics and definitions
15. Train business users
16. Deploy to production

### 6.2 Sample SQL for Creating Date Dimension

```sql
CREATE OR REPLACE TABLE gold.dim_date AS
WITH date_spine AS (
  SELECT explode(sequence(
    to_date('2020-01-01'), 
    to_date('2030-12-31'), 
    interval 1 day
  )) AS full_date
)
SELECT
  -- Surrogate key
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  
  -- Full date
  full_date,
  
  -- Day attributes
  dayofweek(full_date) AS day_of_week,
  date_format(full_date, 'EEEE') AS day_name,
  date_format(full_date, 'EEE') AS day_abbr,
  dayofmonth(full_date) AS day_of_month,
  dayofyear(full_date) AS day_of_year,
  
  -- Week attributes
  weekofyear(full_date) AS week_of_year,
  date_trunc('week', full_date) AS week_start_date,
  date_add(date_trunc('week', full_date), 6) AS week_end_date,
  
  -- Month attributes
  month(full_date) AS month_num,
  date_format(full_date, 'MMMM') AS month_name,
  date_format(full_date, 'MMM') AS month_abbr,
  date_trunc('month', full_date) AS month_start_date,
  last_day(full_date) AS month_end_date,
  
  -- Quarter attributes
  quarter(full_date) AS quarter_num,
  CONCAT('Q', quarter(full_date)) AS quarter_name,
  CONCAT(year(full_date), '-Q', quarter(full_date)) AS quarter_year,
  date_trunc('quarter', full_date) AS quarter_start_date,
  
  -- Year attributes
  year(full_date) AS year,
  
  -- Fiscal attributes (assuming fiscal year starts July 1)
  CASE WHEN month(full_date) >= 7 THEN year(full_date) + 1 ELSE year(full_date) END AS fiscal_year,
  CASE 
    WHEN month(full_date) BETWEEN 7 AND 9 THEN 1
    WHEN month(full_date) BETWEEN 10 AND 12 THEN 2
    WHEN month(full_date) BETWEEN 1 AND 3 THEN 3
    ELSE 4
  END AS fiscal_quarter,
  
  -- Business flags
  CASE WHEN dayofweek(full_date) IN (7, 1) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN dayofweek(full_date) NOT IN (7, 1) THEN TRUE ELSE FALSE END AS is_weekday,
  FALSE AS is_holiday,  -- Populate with actual holiday logic
  NULL AS holiday_name,
  CASE WHEN dayofweek(full_date) NOT IN (7, 1) THEN TRUE ELSE FALSE END AS is_business_day,
  
  -- Relative periods
  datediff(full_date, current_date()) AS days_from_today,
  CASE WHEN year(full_date) = year(current_date()) AND month(full_date) = month(current_date()) THEN TRUE ELSE FALSE END AS is_current_month,
  CASE WHEN year(full_date) = year(add_months(current_date(), -1)) AND month(full_date) = month(add_months(current_date(), -1)) THEN TRUE ELSE FALSE END AS is_last_month,
  CASE WHEN year(full_date) = year(current_date()) AND quarter(full_date) = quarter(current_date()) THEN TRUE ELSE FALSE END AS is_current_quarter
  
FROM date_spine;
```

### 6.3 Data Governance Recommendations

1. **Naming Conventions:**
   - Dimensions: `dim_<entity_name>`
   - Facts: `fact_<subject_area>`
   - Measures: Use business-friendly names (revenue, not amt)
   - Keys: `<entity>_key` for surrogate, `<entity>_id` for natural

2. **Documentation:**
   - Create data dictionary for all tables and columns
   - Document metric calculations and business rules
   - Maintain glossary of business terms

3. **Data Quality:**
   - Implement data quality checks on source data
   - Monitor fact-dimension join integrity
   - Track NULL rates and data completeness
   - Set up alerts for anomalies

4. **Performance:**
   - Materialize large views as tables with incremental refresh
   - Partition fact tables by date_key
   - Z-order optimize on common filter columns
   - Cache frequently-used metrics

### 6.4 Sample Query Examples

#### **Sales by Channel and Month**
```sql
SELECT
  d.year,
  d.month_name,
  c.channel_name,
  COUNT(DISTINCT f.transaction_number) AS transaction_count,
  SUM(f.quantity_sold) AS units_sold,
  SUM(f.total_amount) AS total_revenue,
  SUM(f.gross_profit) AS total_profit,
  ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0) * 100, 2) AS margin_pct
FROM gold.fact_sales_unified f
INNER JOIN gold.dim_date d ON f.date_key = d.date_key
INNER JOIN gold.dim_sales_channel c ON f.sales_channel_key = c.channel_key
WHERE d.year = 2024
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

#### **Top 10 Products by Revenue**
```sql
SELECT
  p.product_name,
  p.brand_name,
  p.category_name,
  SUM(f.quantity_sold) AS units_sold,
  SUM(f.total_amount) AS total_revenue,
  SUM(f.gross_profit) AS total_profit,
  ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.total_amount), 0) * 100, 2) AS margin_pct
FROM gold.fact_sales_unified f
INNER JOIN gold.dim_product p ON f.product_key = p.product_key AND p.is_current = TRUE
WHERE f.date_key >= 20240101
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC
LIMIT 10;
```

#### **Customer Segmentation Analysis**
```sql
SELECT
  c.customer_segment,
  c.age_group,
  COUNT(DISTINCT c.customer_key) AS customer_count,
  COUNT(DISTINCT f.transaction_number) AS transaction_count,
  SUM(f.total_amount) AS total_revenue,
  ROUND(SUM(f.total_amount) / COUNT(DISTINCT c.customer_key), 2) AS revenue_per_customer,
  ROUND(COUNT(DISTINCT f.transaction_number) / NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS transactions_per_customer
FROM gold.fact_sales_unified f
INNER JOIN gold.dim_customer c ON f.customer_key = c.customer_key AND c.is_current = TRUE
GROUP BY 1, 2
ORDER BY total_revenue DESC;
```

#### **Inventory Health Report**
```sql
SELECT
  l.location_name,
  p.category_name,
  COUNT(DISTINCT p.product_key) AS total_products,
  SUM(CASE WHEN f.is_out_of_stock THEN 1 ELSE 0 END) AS out_of_stock_count,
  SUM(CASE WHEN f.is_below_safety_stock THEN 1 ELSE 0 END) AS low_stock_count,
  SUM(f.on_hand_quantity) AS total_on_hand,
  SUM(f.available_quantity) AS total_available,
  ROUND(SUM(f.inventory_value_at_cost), 2) AS inventory_value,
  ROUND(SUM(CASE WHEN f.is_out_of_stock THEN 0 ELSE 1 END) / NULLIF(COUNT(*), 0) * 100, 2) AS in_stock_pct
FROM gold.fact_inventory_snapshot f
INNER JOIN gold.dim_location l ON f.location_key = l.location_key AND l.is_current = TRUE
INNER JOIN gold.dim_product p ON f.product_key = p.product_key AND p.is_current = TRUE
WHERE f.is_current = TRUE
GROUP BY 1, 2
ORDER BY inventory_value DESC;
```

#### **Order Fulfillment Performance**
```sql
SELECT
  d.year,
  d.month_name,
  COUNT(DISTINCT f.order_id) AS total_orders,
  SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END) AS completed_orders,
  SUM(CASE WHEN f.is_on_time_delivery THEN 1 ELSE 0 END) AS on_time_orders,
  ROUND(SUM(CASE WHEN f.is_on_time_delivery THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END), 0) * 100, 2) AS on_time_rate,
  ROUND(AVG(f.total_fulfillment_days), 1) AS avg_fulfillment_days,
  ROUND(AVG(f.order_to_allocation_hours), 1) AS avg_allocation_hours,
  SUM(f.total_amount) AS total_order_value
FROM gold.fact_order_fulfillment f
INNER JOIN gold.dim_date d ON f.order_date_key = d.date_key
WHERE d.year = 2024
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## Section 7: Summary & Next Steps

### Key Achievements from This Analysis

✅ **Comprehensive inventory** of 43 silver layer tables  
✅ **Validated data structures** with actual schemas and volumes  
✅ **Identified critical gaps** (date dimension, sales channel, unified facts)  
✅ **Designed conformed dimensions** with business-friendly names  
✅ **Created fact table designs** with all necessary measures  
✅ **Defined 30+ business metrics** and KPIs  
✅ **Provided sample SQL** for implementation  

### Recommended Semantic Layer Structure

```
gold/
├── Dimensions (Conformed)
│   ├── dim_date ⭐ (NEW - Critical)
│   ├── dim_sales_channel ⭐ (NEW - Important)
│   ├── dim_product (from dim_sap_prd)
│   ├── dim_customer (from dim_sfcc_cust)
│   ├── dim_location (from dim_ncr_location)
│   ├── dim_employee (from dim_sap_user)
│   ├── dim_payment_method (from dim_ncr_pmt_method)
│   ├── dim_brand (from dim_sap_brand)
│   ├── dim_category (from dim_sap_cat)
│   ├── dim_supplier (from dim_sap_sup)
│   └── dim_carrier (from dim_sap_carrier)
│
├── Fact Tables (Subject Areas)
│   ├── fact_sales_unified ⭐ (POS + E-commerce)
│   ├── fact_inventory_snapshot (Store inventory)
│   ├── fact_order_fulfillment (E-commerce lifecycle)
│   ├── fact_purchase_orders (Supplier orders)
│   ├── fact_shipments (Warehouse shipments)
│   └── fact_transfers (Inter-location transfers)
│
└── Metrics (Pre-calculated)
    ├── metrics_sales_daily
    ├── metrics_product_performance
    ├── metrics_customer_segments
    └── metrics_inventory_kpis
```

### Priority Actions

**🔴 HIGH PRIORITY (Week 1-2):**
1. Create `dim_date` table
2. Create `dim_sales_channel` table
3. Build `fact_sales_unified` view
4. Create conformed dimension views

**🟡 MEDIUM PRIORITY (Week 3-4):**
5. Build inventory and fulfillment fact views
6. Implement data quality checks
7. Create initial dashboards for testing

**🟢 LOW PRIORITY (Week 5+):**
8. Optimize with materialized views
9. Build pre-aggregated metrics
10. Comprehensive documentation

### Success Criteria

- ✅ All fact tables join successfully to conformed dimensions
- ✅ Metrics reconcile with source systems
- ✅ Query performance meets SLA (<5 seconds for standard reports)
- ✅ Business users can self-serve analytics
- ✅ Data definitions are documented and accessible

---

**End of Analysis**

*For questions or clarifications, please review the source inspection script at:*  
`/Users/mehdi.modarressi/Documents/Coding/acme_supermarkets_lhp/acme_supermarkets_lhp/inspect_silver_layer.py`

