-- ============================================================================
-- Create Unified Sales Fact View
-- ============================================================================
-- Description: Combines POS and E-commerce sales into a single unified fact
--              table for cross-channel analytics
-- Sources: 
--   - POS: fct_ncr_pos_txn_ln + fct_ncr_pos_txn_hdr
--   - E-commerce: fct_sfcc_sls_ord_ln + fct_sfcc_sls_ord_hdr_hist
-- Grain: One row per line item (product sold in transaction/order)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_sales_unified AS

-- ============================================================================
-- POS Sales (In-Store Transactions)
-- ============================================================================
SELECT
    -- Date key (references dim_date)
    CAST(date_format(h.txn_datetime, 'yyyyMMdd') AS INT) AS date_key,
    h.txn_datetime AS transaction_datetime,
    
    -- Dimension foreign keys
    1 AS sales_channel_key,                     -- POS channel
    h.store_id AS location_key,                 -- Store location
    l.product_id AS product_key,                -- Product
    h.customer_id AS customer_key,              -- Customer (may be NULL for non-loyalty)
    p.method_id AS payment_method_key,          -- Payment method
    h.cashier_user_id AS employee_key,          -- Cashier
    CAST(NULL AS BIGINT) AS carrier_key,        -- Not applicable for POS
    
    -- Degenerate dimensions (descriptive IDs that don't warrant dimension tables)
    CAST(h.txn_id AS STRING) AS transaction_number,
    CAST(l.line_number AS INT) AS line_number,
    'NCR' AS source_system,
    
    -- Quantity measures
    l.qty AS quantity_sold,
    
    -- Revenue measures
    l.unit_price,
    l.line_discount AS discount_amount,
    l.line_total - l.line_discount AS subtotal_amount,
    -- Prorate tax from header to line level
    ROUND((l.line_total - l.line_discount) * (h.total_tax / NULLIF(h.total_net, 0)), 2) AS tax_amount,
    l.line_total AS total_amount,
    
    -- Cost and profitability measures (from product dimension)
    prd.base_cost AS unit_cost,
    l.qty * prd.base_cost AS total_cost,
    l.line_total - (l.qty * prd.base_cost) AS gross_profit,
    ROUND((l.line_total - (l.qty * prd.base_cost)) / NULLIF(l.line_total, 0) * 100, 2) AS margin_pct,
    
    -- Status flags
    FALSE AS is_returned,
    FALSE AS is_cancelled,
    'completed' AS fulfillment_status,
    h.status AS transaction_status,
    h.payment_status
    
FROM acme_supermarkets.edw_silver.fct_ncr_pos_txn_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_ncr_pos_txn_hdr h 
    ON l.txn_id = h.txn_id
LEFT JOIN acme_supermarkets.edw_silver.fct_ncr_pos_txn_pmt p 
    ON h.txn_id = p.txn_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd prd 
    ON l.product_id = prd.product_id 
    AND prd.__END_AT IS NULL

UNION ALL

-- ============================================================================
-- E-commerce Sales (Online Orders)
-- ============================================================================
SELECT
    -- Date key (references dim_date)
    CAST(date_format(h.order_date, 'yyyyMMdd') AS INT) AS date_key,
    h.order_date AS transaction_datetime,
    
    -- Dimension foreign keys
    2 AS sales_channel_key,                     -- E-commerce channel
    h.warehouse_id AS location_key,             -- Fulfillment warehouse
    l.product_id AS product_key,                -- Product
    h.customer_id AS customer_key,              -- Customer
    h.payment_method_id AS payment_method_key,  -- Payment method
    CAST(NULL AS BIGINT) AS employee_key,       -- Not applicable for e-commerce
    h.carrier_id AS carrier_key,                -- Shipping carrier
    
    -- Degenerate dimensions
    h.order_number AS transaction_number,
    CAST(l.line_number AS INT) AS line_number,
    'SFCC' AS source_system,
    
    -- Quantity measures
    l.qty AS quantity_sold,
    
    -- Revenue measures
    l.unit_price,
    l.discount_amount,
    l.line_total - l.discount_amount - l.tax_amount AS subtotal_amount,
    l.tax_amount,
    l.line_total AS total_amount,
    
    -- Cost and profitability measures
    prd.base_cost AS unit_cost,
    l.qty * prd.base_cost AS total_cost,
    l.line_total - (l.qty * prd.base_cost) AS gross_profit,
    ROUND((l.line_total - (l.qty * prd.base_cost)) / NULLIF(l.line_total, 0) * 100, 2) AS margin_pct,
    
    -- Status flags
    FALSE AS is_returned,
    CASE WHEN l.cancelled_qty > 0 THEN TRUE ELSE FALSE END AS is_cancelled,
    l.status AS fulfillment_status,
    h.status AS transaction_status,
    CAST(NULL AS STRING) AS payment_status
    
FROM acme_supermarkets.edw_silver.fct_sfcc_sls_ord_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sfcc_sls_ord_hdr_hist h 
    ON l.order_id = h.order_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd prd 
    ON l.product_id = prd.product_id 
    AND prd.__END_AT IS NULL;

-- ============================================================================
-- Test the unified fact view
-- ============================================================================

-- Row counts by channel
SELECT 
  sc.channel_name,
  COUNT(*) AS line_item_count,
  COUNT(DISTINCT f.transaction_number) AS transaction_count,
  SUM(f.quantity_sold) AS total_units,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(SUM(f.gross_profit), 2) AS total_profit,
  ROUND(AVG(f.margin_pct), 2) AS avg_margin_pct
FROM acme_supermarkets.edw_gold.fact_sales_unified f
LEFT JOIN acme_supermarkets.edw_gold.dim_sales_channel sc 
    ON f.sales_channel_key = sc.channel_key
GROUP BY sc.channel_name
ORDER BY total_revenue DESC;

-- Sample transactions
SELECT 
  d.full_date,
  sc.channel_name,
  p.product_name,
  f.quantity_sold,
  f.total_amount,
  f.gross_profit,
  f.margin_pct
FROM acme_supermarkets.edw_gold.fact_sales_unified f
LEFT JOIN acme_supermarkets.edw_gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN acme_supermarkets.edw_gold.dim_sales_channel sc ON f.sales_channel_key = sc.channel_key
LEFT JOIN acme_supermarkets.edw_gold.dim_product p ON f.product_key = p.product_id AND p.is_current = TRUE
LIMIT 10;

