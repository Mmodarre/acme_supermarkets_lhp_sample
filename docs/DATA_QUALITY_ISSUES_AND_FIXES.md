# ACME Supermarkets - Data Quality Issues & Required Fixes

**Report Date**: 2025-10-22  
**Scope**: Silver & Gold Layer Data Quality Assessment  
**Impact**: Affects all metric views and business reporting

---

## Executive Summary

During implementation of the semantic layer and metric views, **8 critical data quality issues** were identified that prevent accurate business reporting. This document outlines:
1. All observed issues with impact assessment
2. Missing data required for meaningful metrics
3. Workarounds currently in place (must be reverted after fixes)

**Critical**: 99% of customers and 100% of nutrition data are missing. Warehouse key mismatches cause 100% of fulfillment metrics to show "Unknown Warehouse". On Time Delivery Rate always returns 0% due to missing estimated delivery dates.

---

## Part 1: Data Quality Issues Observed

### 🔴 **Issue 1: Warehouse Dimension - Key Mismatch (CRITICAL)**

**Problem**: Foreign keys in fact tables don't exist in the warehouse dimension.

**Details**:
- `fact_order_fulfillment.warehouse_key = 51`
- `fact_transfer_orders.source_warehouse_key = 51` (and others)
- `dim_warehouse.warehouse_id` only has values: `1, 2, 3, 4`

**Impact**:
- ✗ All e-commerce orders show "Unknown Warehouse" (37,701 orders, $24M revenue)
- ✗ All transfer orders can't be analyzed by warehouse
- ✗ `mv_ecommerce_fulfillment` - No warehouse location data
- ✗ `mv_supply_chain` - Returns 0 rows due to missing warehouse join

**Root Cause**: Warehouse ID assignment mismatch between SAP (products 1-4) and SFCC (uses 51).

**Fix Required**:
```sql
-- Check all warehouse keys used in facts
SELECT DISTINCT warehouse_id FROM fct_sfcc_sls_ord_hdr_hist; -- Returns: 51
SELECT DISTINCT source_warehouse_key FROM fct_sap_xfer_ord_hdr; -- Returns: various

-- Add missing warehouses to dim_warehouse OR
-- Fix warehouse_id mapping in SFCC → SAP integration
```

---

### 🔴 **Issue 2: Customer Dimension - 99% Missing Data (CRITICAL)**

**Problem**: Only 160 out of 15,062 customers (1%) exist in `dim_customer`.

**Details**:
- Total distinct customers in `fact_sales_unified`: **15,062**
- Customers in `dim_customer` (is_current=TRUE): **160**
- **Missing: 14,902 customers (99%)**

**Impact**:
- ✗ Customer segmentation metrics return NULL for 99% of sales
- ✗ `mv_customer_segmentation` - No customer attributes (age, segment, tenure)
- ✗ `mv_profitability` - Can't analyze by customer segment
- ✗ `mv_strategic_kpis` - Cross-channel purchase rate incomplete

**Affected Metric Views**:
- `mv_customer_segmentation` - Primary impact
- `mv_profitability`
- `mv_strategic_kpis`
- `mv_omnichannel_performance`
- `mv_payment_innovation`
- `mv_ecommerce_fulfillment`
- `mv_financial_planning`

**Root Cause**: Customer dimension only loaded from one source (likely SFCC), missing NCR POS customers.

**Fix Required**:
```sql
-- Verify customer sources
SELECT source_system, COUNT(DISTINCT customer_key) 
FROM fact_sales_unified 
GROUP BY source_system;
-- NCR (POS): ~14,900 customers
-- SFCC (E-commerce): ~160 customers

-- Action: Load NCR customers into dim_customer
-- OR create surrogate customers for missing IDs
```

---

### 🔴 **Issue 3: Product Nutrition Data - 100% Missing (HIGH)**

**Problem**: `nutrition_data` column exists in `dim_product` but is NULL for all 13,939 products.

**Details**:
- Total products: **13,939**
- Products with nutrition_data: **0 (0%)**

**Impact**:
- ✗ "Health Category Leadership" metric always returns 0%
- ✗ Can't identify health-focused products
- ✗ Can't calculate nutrition-based KPIs

**Affected Measures**:
- `mv_strategic_kpis.Health Category Leadership` - Returns 0%

**Root Cause**: `nutrition_data` field not populated during SAP product ingestion.

**Fix Required**:
```sql
-- Check source table
SELECT COUNT(*), COUNT(nutrition_data) 
FROM raw.sap_products;

-- If source has data: Fix bronze/silver mapping
-- If source lacks data: Populate from external nutrition database
```

---

### 🟠 **Issue 4: Order Fulfillment - Negative Timestamps (MEDIUM)**

**Problem**: 1,251 shipped orders (34% of shipped) have `last_update_dttm < order_date`.

**Details**:
- Total shipped orders: **3,658**
- Orders with negative ship time: **1,251 (34%)**
- Pattern: Orders placed after 6 PM show shipped date from previous day at 7 PM

**Example**:
```
order_date: 2024-01-06 18:59:00
shipped_date: 2024-01-05 19:00:00
Result: -24 hours ship time
```

**Impact**:
- ⚠️ Ship time metrics exclude 34% of shipped orders
- ⚠️ Averages based on smaller sample size
- ⚠️ Same-day fulfillment % is underreported

**Root Cause**: Batch processing timestamp (7 PM daily) being used instead of actual shipment time.

**Current Workaround**: Set ship time to NULL when negative (see Part 3).

**Fix Required**:
```sql
-- Option 1: Join to shipment tracking table for real timestamps
SELECT o.order_id, s.ship_date
FROM fct_sfcc_sls_ord_hdr_hist o
JOIN fct_sfcc_shmpt_hdr_snapshot s ON o.order_id = s.order_id

-- Option 2: Fix batch job to capture actual shipment timestamps
-- Instead of using batch run time (7 PM), use order status change timestamp
```

---

### 🟠 **Issue 5: Created_at = ETL Timestamp, Not Business Date (MEDIUM)**

**Problem**: `created_at` column in all SFCC tables is the ETL load timestamp, not a business event date.

**Details**:
- Business dates (`order_date`): 2023-12-31 to 2024-01-07
- All `created_at` values: **2025-10-21** (today's ETL run)

**Impact**:
- ⚠️ Originally used `created_at` for "allocation date" - showed all orders allocated today
- ⚠️ Any metric using `created_at` is incorrect

**Current Workaround**: Removed allocation measures entirely, use `last_update_dttm` instead (see Part 3).

**Fix Required**:
```sql
-- Identify correct business date columns
-- For orders: order_date, updated_at, last_update_dttm are valid business dates
-- For allocation: Need separate allocation_date column or join to warehouse allocation log

-- Recommendation: Add metadata columns
ALTER TABLE fct_sfcc_sls_ord_hdr_hist ADD COLUMN allocated_at TIMESTAMP;
-- Populate from warehouse allocation system
```

---

### 🟢 **Issue 6: No Real Allocation Date in Source (LOW)**

**Problem**: E-commerce order tables don't track when orders were allocated to warehouse inventory.

**Details**:
- `order_date` exists
- `updated_at` exists (general update, not specific to allocation)
- No `allocated_at` or `inventory_allocated_date`

**Impact**:
- ✗ Can't calculate "Order to Allocation" time
- ✗ Can't calculate "Allocation to Ship" time
- ✗ `mv_ecommerce_fulfillment` missing allocation metrics

**Current Workaround**: Removed allocation measures from fact and metric views (see Part 3).

**Fix Required**:
```sql
-- Check if allocation events exist in separate table
SELECT * FROM warehouse_allocation_log LIMIT 5;

-- If exists: Join to get allocation timestamp
-- If not: Implement allocation tracking in warehouse system
```

---

### 🟢 **Issue 7: Surrogate Keys Not Populated in Facts (LOW)**

**Problem**: Fact tables reference dimension natural keys, not surrogate keys.

**Details**:
- Facts use: `customer_id`, `product_id`, `location_id`
- Dimensions have: `customer_key` (surrogate), `customer_id` (natural)
- Joins must be on natural keys, not surrogate keys

**Impact**:
- ⚠️ Joins are slower (string comparison vs integer)
- ⚠️ SCD Type 2 tracking not utilized (always uses `is_current = TRUE`)

**Current Workaround**: All gold layer views join on natural keys (see Part 3).

**Fix Required**:
```sql
-- Option 1: Populate surrogate keys in silver facts
UPDATE fct_ncr_pos_txn_ln 
SET customer_key = (
    SELECT customer_key FROM dim_sfcc_cust 
    WHERE customer_id = fct_ncr_pos_txn_ln.customer_id 
    AND __END_AT IS NULL
);

-- Option 2: Accept natural key joins (current approach is fine)
```

---

### 🔴 **Issue 8: Estimated Delivery Date - 100% Missing (HIGH)**

**Problem**: `estimated_delivery_date` is NULL for 100% of orders, making "On Time Delivery Rate" always 0%.

**Details**:
- Total orders: **37,701**
- Orders with `estimated_delivery_date`: **0 (0%)**
- Orders with `actual_delivery_date`: **7,640 (20.3%)**
- Result: All 37,701 orders have `is_on_time_delivery = FALSE`

**Impact**:
- ✗ "On Time Delivery Rate" always returns **0%** for all fulfillment statuses
- ✗ Can't measure delivery performance against promises
- ✗ Can't identify late deliveries vs on-time deliveries
- ✗ Customer satisfaction metrics incomplete

**Affected Measures**:
- `mv_ecommerce_fulfillment.On Time Delivery Rate` - Always 0%
- `mv_ecommerce_fulfillment.On Time Delivery Count` - Always 0

**Current Logic** (in `fact_order_fulfillment`):
```sql
CASE 
    WHEN o.estimated_delivery_date IS NOT NULL 
         AND o.actual_delivery_date IS NOT NULL
         AND o.actual_delivery_date <= o.estimated_delivery_date
    THEN TRUE
    WHEN o.actual_delivery_date IS NULL AND o.estimated_delivery_date > CURRENT_DATE()
    THEN NULL  -- Still pending
    ELSE FALSE
END AS is_on_time_delivery
```

Since `estimated_delivery_date` is always NULL, this always evaluates to FALSE.

**Root Cause**: Estimated delivery dates not captured/stored during order placement in SFCC system.

**Fix Required**:
```sql
-- Check if estimated dates exist anywhere
SELECT 
    COUNT(*) as total,
    COUNT(estimated_delivery_date) as with_estimated,
    COUNT(expected_date) as with_expected
FROM raw.sfcc_orders;

-- If data doesn't exist: 
-- Option 1: Calculate estimated date based on business rules
--   e.g., order_date + standard_shipping_days
ALTER TABLE fct_sfcc_sls_ord_hdr_hist 
ADD COLUMN calculated_estimated_delivery DATE AS (
    DATE_ADD(order_date, 
        CASE 
            WHEN shipping_method = 'standard' THEN 5
            WHEN shipping_method = 'express' THEN 2
            WHEN shipping_method = 'overnight' THEN 1
            ELSE 7
        END
    )
);

-- Option 2: Populate from order confirmation emails/logs
-- Option 3: Use shipment tracking expected_delivery_date as proxy
SELECT order_id, expected_delivery_date 
FROM fct_sfcc_shmpt_hdr_snapshot;
```

**Workaround Impact**:
- Currently NO workaround in place
- Metric returns 0% which is misleading (should be NULL or "Not Available")

**TODO - Add Workaround**:
Update `fact_order_fulfillment.sql` to set `is_on_time_delivery = NULL` when estimated date is missing:
```sql
CASE 
    WHEN o.estimated_delivery_date IS NULL 
    THEN NULL  -- Can't determine if on-time without estimated date
    WHEN o.estimated_delivery_date IS NOT NULL 
         AND o.actual_delivery_date IS NOT NULL
         AND o.actual_delivery_date <= o.estimated_delivery_date
    THEN TRUE
    WHEN o.estimated_delivery_date IS NOT NULL 
         AND o.actual_delivery_date IS NOT NULL
    THEN FALSE
    WHEN o.estimated_delivery_date IS NOT NULL 
         AND o.actual_delivery_date IS NULL 
         AND CURRENT_DATE() <= o.estimated_delivery_date
    THEN NULL  -- Still pending
    ELSE FALSE  -- Late
END AS is_on_time_delivery
```

---

## Part 2: Missing Values Required for Meaningful Metrics

### By Metric View:

#### **`mv_omnichannel_performance`**
**Status**: 🟡 Partially Functional  
**Missing**:
- ✗ 14,902 customers (99%) - can't analyze cross-channel behavior
- ✗ Customer segments NULL for 99% of transactions

**Metrics Affected**:
- Cross-Channel Purchase Rate - Incomplete (only 1% of customers)
- Customer Purchase Frequency - Skewed (missing 99%)
- All customer segmentation metrics

---

#### **`mv_store_network`**
**Status**: 🟡 Partially Functional  
**Missing**:
- ⚠️ Employee dimension may have gaps (not verified)
- Location data is complete

**Metrics Affected**:
- Potentially: Revenue per Employee (if employee keys missing)

---

#### **`mv_supply_chain`**
**Status**: 🔴 Non-Functional  
**Missing**:
- ✗ Warehouse key 51 (and potentially others)

**Metrics Affected**:
- ALL metrics return 0 rows due to missing warehouse join
- Warehouse Transfer Efficiency: 0
- Inventory Turnover: 0

**Current State**: Entire metric view returns no data.

---

#### **`mv_customer_segmentation`**
**Status**: 🔴 Mostly Non-Functional  
**Missing**:
- ✗ 14,902 customers (99%)
- ✗ Customer attributes: age, segment, tenure, gender

**Metrics Affected**:
- Customer Lifetime Value: Only 1% of customers
- Average Transaction Value by Segment: NULL for 99%
- All segmentation KPIs incomplete

---

#### **`mv_payment_innovation`**
**Status**: 🟡 Partially Functional  
**Missing**:
- ✗ 14,902 customers (99%)
- Payment method data appears complete

**Metrics Affected**:
- Digital Payment Adoption by Customer Segment: Incomplete

---

#### **`mv_ecommerce_fulfillment`**
**Status**: 🟠 Functional with Gaps  
**Missing**:
- ✗ Warehouse location (shows "Unknown")
- ✗ 34% of shipped orders have invalid timestamps
- ✗ Allocation date/metrics

**Metrics Affected**:
- Warehouse-level metrics: Can't break down by location
- Same-Day Fulfillment: Underreported (excludes 34% with bad timestamps)

---

#### **`mv_strategic_kpis`**
**Status**: 🟠 Mostly Functional  
**Missing**:
- ✗ 14,902 customers (99%)
- ✗ Product nutrition data (100%)

**Metrics Affected**:
- Health Category Leadership: Always 0%
- Cross-Channel Purchase Rate: Only 1% of customers

---

#### **`mv_profitability`**
**Status**: 🟡 Partially Functional  
**Missing**:
- ✗ 14,902 customers (99%)

**Metrics Affected**:
- Channel Profitability Comparison: Incomplete
- Customer Segment Margin: NULL for 99%

---

#### **`mv_revenue_quality` & `mv_financial_planning`**
**Status**: 🟢 Mostly Functional  
**Missing**:
- ⚠️ Some customer data gaps

**Metrics Affected**:
- Minimal impact, aggregate metrics work

---

### Summary Table:

| Metric View | Status | Critical Missing Data | % Functional |
|-------------|--------|----------------------|--------------|
| mv_supply_chain | 🔴 | Warehouse keys | 0% |
| mv_customer_segmentation | 🔴 | 99% customers | 1% |
| mv_omnichannel_performance | 🟡 | 99% customers | 20% |
| mv_profitability | 🟡 | 99% customers | 30% |
| mv_payment_innovation | 🟡 | 99% customers | 40% |
| mv_ecommerce_fulfillment | 🟠 | Warehouse keys, timestamps | 60% |
| mv_strategic_kpis | 🟠 | Customers, nutrition | 70% |
| mv_store_network | 🟡 | Minimal | 80% |
| mv_revenue_quality | 🟢 | Minimal | 90% |
| mv_financial_planning | 🟢 | Minimal | 90% |

**Overall System Health**: 🟠 **50% Functional**

---

## Part 3: Workarounds Implemented (Must Revert After Fixes)

### **Workaround 1: Removed `is_current` Filter on Warehouse Joins**

**Files**:
- `metric_views/mv_ecommerce_fulfillment.yaml` (line 18)
- `metric_views/mv_supply_chain.yaml` (line 20)

**Original Code**:
```yaml
filter: dim_warehouse.is_current = TRUE AND dim_customer.is_current = TRUE
```

**Current Workaround**:
```yaml
filter: dim_customer.is_current = TRUE  # Removed dim_warehouse.is_current
```

**Reason**: Warehouse keys don't match, so `is_current` filter drops all rows.

**TODO - Revert After Fix**:
```yaml
# Once warehouse keys are fixed, restore:
filter: dim_warehouse.is_current = TRUE AND dim_customer.is_current = TRUE
```

---

### **Workaround 2: Set Ship Time to NULL for Non-Shipped & Invalid Timestamps**

**Files**:
- `sql/semantic_layer/17_create_fact_order_fulfillment.sql` (lines 42-51)

**Original Logic**:
```sql
-- Calculate for all orders
order_to_ship_hours = (last_update_dttm - order_date) / 3600
```

**Current Workaround**:
```sql
CASE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')  -- Only shipped
         AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)  -- No negative
    THEN ROUND((UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0, 2)
    ELSE NULL  -- Allocated or bad timestamps
END AS order_to_ship_hours
```

**Reason**: 
1. Allocated orders don't have ship time yet
2. 1,251 orders have negative timestamps (batch processing issue)

**TODO - Revert After Fix**:
```sql
-- Once timestamps are fixed, remove the negative check:
CASE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
    THEN ROUND((UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0, 2)
    ELSE NULL
END AS order_to_ship_hours
-- No need for: AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
```

---

### **Workaround 3: Removed Allocation Measures Entirely**

**Files**:
- `sql/semantic_layer/17_create_fact_order_fulfillment.sql`
- `metric_views/mv_ecommerce_fulfillment.yaml`

**Original Design**:
```sql
allocated_date,
allocated_date_key,
order_to_allocation_hours,
allocation_to_ship_hours
```

**Current Workaround**:
```sql
-- All allocation measures removed
-- Only have: order_to_ship_hours, order_to_completion_days
```

**Reason**: No real allocation date in source data, `created_at` is ETL timestamp.

**TODO - Restore After Fix**:
```sql
-- Once allocation_date is available in source:
o.allocated_at AS allocated_date,
CAST(date_format(o.allocated_at, 'yyyyMMdd') AS INT) AS allocated_date_key,
ROUND((UNIX_TIMESTAMP(o.allocated_at) - UNIX_TIMESTAMP(o.order_date)) / 3600.0, 2) AS order_to_allocation_hours,
ROUND((UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.allocated_at)) / 3600.0, 2) AS allocation_to_ship_hours
```

Then restore in metric view:
```yaml
measures:
  - name: Order to Allocation Hours
    expr: AVG(order_to_allocation_hours)
  - name: Allocation to Ship Hours
    expr: AVG(allocation_to_ship_hours)
```

---

### **Workaround 4: Used Natural Keys Instead of Surrogate Keys in Joins**

**Files**:
- All `sql/semantic_layer/*_create_*.sql` files

**Design Principle**:
```sql
-- Fact tables store natural keys (customer_id, product_id, location_id)
-- Dimensions have both surrogate (customer_key) and natural (customer_id)
```

**Current Implementation**:
```sql
-- All gold layer joins use natural keys:
JOIN dim_customer d ON f.customer_key = d.customer_id AND d.is_current = TRUE
```

**Reason**: 
- Surrogate keys not populated in silver facts
- Natural keys are available and work correctly

**TODO - Consider After Fix**:
```sql
-- Option 1: Populate surrogate keys in silver facts
-- Option 2: Keep current approach (natural key joins work fine)
-- Decision: Likely keep current approach unless performance issues
```

---

### **Workaround 5: Added `is_shipped` Flag for Filtering**

**Files**:
- `sql/semantic_layer/17_create_fact_order_fulfillment.sql` (lines 89-93)
- `metric_views/mv_ecommerce_fulfillment.yaml` (line 39-40)

**Addition**:
```sql
CASE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
    THEN TRUE
    ELSE FALSE
END AS is_shipped
```

```yaml
dimensions:
  - name: Is Shipped
    expr: is_shipped
```

**Reason**: Users need to filter out non-shipped orders from reports.

**TODO - Keep After Fix**:
```text
This is actually a good design pattern, not a workaround.
KEEP this even after data is fixed - provides useful filtering capability.
```

---

### **Workaround 6: Set Performance Flags to NULL for Invalid Data**

**Files**:
- `sql/semantic_layer/17_create_fact_order_fulfillment.sql` (lines 62-87)

**Original Logic**:
```sql
is_same_day_fulfillment = (ship_hours <= 24) ? TRUE : FALSE
```

**Current Workaround**:
```sql
CASE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
         AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)  -- Valid timestamp
         AND (UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0 <= 24 
    THEN TRUE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
         AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
    THEN FALSE
    ELSE NULL  -- Not shipped or invalid timestamp
END AS is_same_day_fulfillment
```

**Reason**: Invalid timestamps would cause FALSE flags, skewing percentages.

**TODO - Simplify After Fix**:
```sql
-- Once timestamps are fixed:
CASE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
         AND (UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0 <= 24 
    THEN TRUE 
    WHEN o.status IN ('shipped', 'completed', 'delivered')
    THEN FALSE
    ELSE NULL
END AS is_same_day_fulfillment
-- Remove: AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
```

---

### **Workaround 7: Used LEFT JOIN Instead of INNER JOIN for Dimensions**

**Files**:
- Most gold layer view definitions

**Pattern**:
```sql
FROM fact_table f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_id AND c.is_current = TRUE
LEFT JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_id AND w.is_current = TRUE
```

**Reason**: INNER JOIN would drop 99% of rows due to missing dimension data.

**TODO - Consider INNER JOIN After Fix**:
```sql
-- Once dimension data is complete:
-- Evaluate if INNER JOIN is appropriate for data quality enforcement
-- LEFT JOIN is safer, but INNER JOIN catches missing dimension references
```

---

## Recommended Fix Priority

### **Phase 1 - Critical (Do First)** 🔴

1. **Fix Warehouse Key Mapping**
   - Impact: Unlocks `mv_supply_chain`, `mv_ecommerce_fulfillment`
   - Effort: Medium (requires SAP-SFCC mapping table or data correction)

2. **Load Missing Customers (14,902 NCR customers)**
   - Impact: Unlocks 99% of customer metrics across all views
   - Effort: High (ETL change to load NCR customers into dimension)

### **Phase 2 - High Priority** 🟠

3. **Fix Shipment Timestamps**
   - Impact: Accurate fulfillment metrics for 100% of shipped orders
   - Effort: Medium (join to shipment table or fix batch processing)

4. **Add Allocation Date Column**
   - Impact: Restores allocation-related metrics
   - Effort: High (requires warehouse system integration)

### **Phase 3 - Nice to Have** 🟢

5. **Populate Estimated Delivery Dates**
   - Impact: Enables On Time Delivery Rate metric
   - Effort: Medium (calculate from business rules or join to shipment tracking)

6. **Populate Product Nutrition Data**
   - Impact: Enables health category metrics
   - Effort: High (data acquisition + ETL)

7. **Populate Surrogate Keys in Facts**
   - Impact: Performance improvement, SCD Type 2 support
   - Effort: Medium (ETL enhancement)

---

## Verification Queries After Fixes

Once data is fixed, run these queries to verify:

### **Verify Warehouse Keys**
```sql
SELECT 'Facts', COUNT(DISTINCT warehouse_key) FROM fact_order_fulfillment
UNION ALL
SELECT 'Dimension', COUNT(DISTINCT warehouse_id) FROM dim_warehouse;
-- Should show same count
```

### **Verify Customer Completeness**
```sql
SELECT 
    COUNT(DISTINCT f.customer_key) as fact_customers,
    COUNT(DISTINCT CASE WHEN d.customer_id IS NOT NULL THEN f.customer_key END) as matched_customers,
    ROUND(COUNT(DISTINCT CASE WHEN d.customer_id IS NOT NULL THEN f.customer_key END) * 100.0 / 
          COUNT(DISTINCT f.customer_key), 1) as match_pct
FROM fact_sales_unified f
LEFT JOIN dim_customer d ON f.customer_key = d.customer_id AND d.is_current = TRUE;
-- match_pct should be > 95%
```

### **Verify Negative Timestamps Fixed**
```sql
SELECT 
    COUNT(*) as shipped_orders,
    COUNT(CASE WHEN last_update_dttm < order_date THEN 1 END) as negative_timestamps
FROM fct_sfcc_sls_ord_hdr_hist
WHERE status IN ('shipped', 'completed', 'delivered') AND __END_AT IS NULL;
-- negative_timestamps should be 0
```

### **Verify Nutrition Data Populated**
```sql
SELECT 
    COUNT(*) as total_products,
    COUNT(nutrition_data) as with_nutrition,
    ROUND(COUNT(nutrition_data) * 100.0 / COUNT(*), 1) as nutrition_pct
FROM dim_product
WHERE is_current = TRUE;
-- nutrition_pct should be > 80%
```

### **Verify Estimated Delivery Dates Populated**
```sql
SELECT 
    COUNT(*) as total_orders,
    COUNT(estimated_delivery_date) as with_estimated,
    COUNT(actual_delivery_date) as with_actual,
    COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END) as on_time_count,
    ROUND(COUNT(estimated_delivery_date) * 100.0 / COUNT(*), 1) as estimated_pct
FROM fact_order_fulfillment;
-- estimated_pct should be > 80%
-- on_time_count should be > 0
```

---

## Contact & Questions

For questions about this document or data quality issues:
- Review this document: `DATA_QUALITY_ISSUES_AND_FIXES.md`
- Check metric view definitions: `metric_views/*.yaml`
- Review gold layer SQL: `sql/semantic_layer/*.sql`

**Generated**: 2025-10-22 by Semantic Layer Implementation

