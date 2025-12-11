-- ============================================================================
-- Create Carrier Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed carrier dimension for e-commerce fulfillment
--              and shipping analytics
-- Source: dim_sap_carrier
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_carrier AS
SELECT
    -- Keys
    c.carrier_key,
    c.carrier_id,
    
    -- Carrier attributes
    c.name AS carrier_name,
    c.contact_info,
    c.status AS carrier_status,
    
    -- Audit columns
    c.last_update_dttm,
    
    -- SCD Type 2 fields
    c.__START_AT AS effective_from_date,
    c.__END_AT AS effective_to_date,
    CASE WHEN c.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_carrier c;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Count carriers
SELECT 
  COUNT(*) AS total_carriers,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_carriers,
  COUNT(CASE WHEN is_current = FALSE THEN 1 END) AS historical_carriers
FROM acme_supermarkets.edw_gold.dim_carrier;

-- List current carriers
SELECT 
  carrier_key,
  carrier_id,
  carrier_name,
  carrier_status,
  contact_info
FROM acme_supermarkets.edw_gold.dim_carrier
WHERE is_current = TRUE
ORDER BY carrier_name;

