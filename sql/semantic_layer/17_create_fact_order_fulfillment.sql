-- ============================================================================
-- Create E-commerce Order Fulfillment Fact View
-- ============================================================================
-- Description: Creates e-commerce order fulfillment fact for order-to-ship,
--              fulfillment success rate, and warehouse performance metrics
-- Source: fct_sfcc_sls_ord_hdr_hist
-- Grain: One row per order (header level)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_order_fulfillment AS
SELECT
    -- Date keys (for time-series analysis)
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
    CAST(date_format(o.last_update_dttm, 'yyyyMMdd') AS INT) AS shipped_date_key,
    CAST(date_format(o.actual_delivery_date, 'yyyyMMdd') AS INT) AS completed_date_key,
    
    -- Date/time stamps
    o.order_date,
    o.last_update_dttm AS shipped_date,
    o.actual_delivery_date AS completed_date,
    
    -- Dimension foreign keys
    o.order_id,
    o.customer_id AS customer_key,
    o.warehouse_id AS warehouse_key,
    2 AS sales_channel_key,  -- E-commerce
    
    -- Degenerate dimensions
    o.order_number,
    o.status AS order_status,
    o.fulfillment_status,
    'SFCC' AS source_system,
    
    -- Revenue measures
    o.subtotal,
    o.tax_amount,
    o.shipping_amount,
    o.discount_amount,
    o.total_amount,
    
    -- Fulfillment timing measures (in hours and days)
    -- Only calculate ship time for orders that have actually shipped (status != 'allocated')
    -- Set to NULL if negative (data quality issue - shipped_date before order_date)
    CASE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
             AND o.last_update_dttm IS NOT NULL 
             AND o.order_date IS NOT NULL 
             AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
        THEN ROUND((UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0, 2)
        ELSE NULL 
    END AS order_to_ship_hours,
    
    -- Only calculate completion time for delivered orders
    CASE 
        WHEN o.status IN ('completed', 'delivered')
             AND o.actual_delivery_date IS NOT NULL 
             AND o.order_date IS NOT NULL 
        THEN DATEDIFF(o.actual_delivery_date, DATE(o.order_date))
        ELSE NULL 
    END AS order_to_completion_days,
    
    -- Fulfillment performance flags (only for shipped orders with valid timestamps)
    CASE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
             AND o.last_update_dttm IS NOT NULL 
             AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
             AND (UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0 <= 24 
        THEN TRUE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
             AND o.last_update_dttm IS NOT NULL 
             AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
        THEN FALSE
        ELSE NULL  -- Not yet shipped or invalid timestamp
    END AS is_same_day_fulfillment,
    
    CASE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
             AND o.last_update_dttm IS NOT NULL 
             AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
             AND (UNIX_TIMESTAMP(o.last_update_dttm) - UNIX_TIMESTAMP(o.order_date)) / 3600.0 <= 48 
        THEN TRUE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
             AND o.last_update_dttm IS NOT NULL 
             AND UNIX_TIMESTAMP(o.last_update_dttm) >= UNIX_TIMESTAMP(o.order_date)
        THEN FALSE
        ELSE NULL  -- Not yet shipped or invalid timestamp
    END AS is_next_day_fulfillment,
    
    -- Flag to indicate if order has been shipped (useful for filtering)
    CASE 
        WHEN o.status IN ('shipped', 'completed', 'delivered')
        THEN TRUE
        ELSE FALSE
    END AS is_shipped,
    
    CASE 
        WHEN o.status = 'completed' AND o.fulfillment_status = 'fulfilled' 
        THEN TRUE 
        ELSE FALSE 
    END AS is_successfully_fulfilled,
    
    -- On-time delivery tracking (NULL if estimated date missing - see Issue #8)
    CASE 
        WHEN o.estimated_delivery_date IS NULL 
        THEN NULL  -- Can't determine if on-time without estimated date
        WHEN o.actual_delivery_date IS NOT NULL
             AND o.actual_delivery_date <= o.estimated_delivery_date
        THEN TRUE
        WHEN o.actual_delivery_date IS NOT NULL
        THEN FALSE  -- Delivered late
        WHEN CURRENT_DATE() <= o.estimated_delivery_date
        THEN NULL  -- Still pending, not late yet
        ELSE FALSE  -- Past estimated date, still not delivered (late)
    END AS is_on_time_delivery,
    
    -- Delivery dates
    o.estimated_delivery_date,
    o.actual_delivery_date,
    
    -- Order counts (for aggregation)
    1 AS order_count,
    
    -- Audit columns
    o.last_update_dttm
    
FROM acme_supermarkets.edw_silver.fct_sfcc_sls_ord_hdr_hist o
WHERE o.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Overall order fulfillment summary
SELECT 
  COUNT(*) AS total_orders,
  COUNT(CASE WHEN is_successfully_fulfilled THEN 1 END) AS fulfilled_orders,
  ROUND(COUNT(CASE WHEN is_successfully_fulfilled THEN 1 END) * 100.0 / COUNT(*), 2) AS fulfillment_rate_pct,
  ROUND(AVG(order_to_ship_hours), 2) AS avg_order_to_ship_hours,
  COUNT(CASE WHEN is_same_day_fulfillment THEN 1 END) AS same_day_orders,
  ROUND(SUM(total_amount), 2) AS total_order_value
FROM acme_supermarkets.edw_gold.fact_order_fulfillment;

-- Fulfillment by warehouse
SELECT 
  w.warehouse_name,
  COUNT(*) AS order_count,
  ROUND(AVG(f.order_to_ship_hours), 2) AS avg_ship_time_hours,
  ROUND(COUNT(CASE WHEN f.is_same_day_fulfillment THEN 1 END) * 100.0 / COUNT(*), 2) AS same_day_pct,
  ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM acme_supermarkets.edw_gold.fact_order_fulfillment f
LEFT JOIN acme_supermarkets.edw_gold.dim_warehouse w 
    ON f.warehouse_key = w.warehouse_id AND w.is_current = TRUE
GROUP BY w.warehouse_name
ORDER BY order_count DESC;

-- Order status distribution
SELECT 
  order_status,
  fulfillment_status,
  COUNT(*) AS order_count,
  ROUND(AVG(order_to_ship_hours), 2) AS avg_ship_time_hours,
  ROUND(SUM(total_amount), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_order_fulfillment
GROUP BY order_status, fulfillment_status
ORDER BY order_count DESC;

-- Daily order fulfillment trend
SELECT 
  d.full_date,
  d.day_name,
  COUNT(*) AS order_count,
  ROUND(AVG(f.order_to_ship_hours), 2) AS avg_ship_time,
  COUNT(CASE WHEN f.is_same_day_fulfillment THEN 1 END) AS same_day_count
FROM acme_supermarkets.edw_gold.fact_order_fulfillment f
LEFT JOIN acme_supermarkets.edw_gold.dim_date d ON f.order_date_key = d.date_key
WHERE d.full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC
LIMIT 30;

