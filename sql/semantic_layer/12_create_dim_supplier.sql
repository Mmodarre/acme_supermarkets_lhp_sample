-- ============================================================================
-- Create Supplier Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed supplier/vendor dimension for procurement
--              and supply chain analytics
-- Source: dim_sap_sup (with payment terms)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_supplier AS
SELECT
    -- Keys
    s.sup_key AS supplier_key,
    s.vendor_id AS supplier_id,
    
    -- Supplier attributes
    s.name AS supplier_name,
    s.tax_id,
    s.status AS supplier_status,
    
    -- Payment terms (denormalized)
    s.payment_terms_id,
    pt.name AS payment_terms_name,
    pt.description AS payment_terms_description,
    pt.days AS payment_terms_days,
    
    -- Audit columns
    s.created_at AS supplier_created_at,
    s.updated_at AS supplier_updated_at,
    
    -- SCD Type 2 fields
    s.__START_AT AS effective_from_date,
    s.__END_AT AS effective_to_date,
    CASE WHEN s.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_sup s
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_pmt_terms pt 
    ON s.payment_terms_id = pt.payment_terms_id;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Count suppliers
SELECT 
  COUNT(*) AS total_suppliers,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_suppliers,
  COUNT(CASE WHEN is_current = FALSE THEN 1 END) AS historical_suppliers
FROM acme_supermarkets.edw_gold.dim_supplier;

-- Sample suppliers with payment terms
SELECT 
  supplier_key,
  supplier_id,
  supplier_name,
  supplier_status,
  payment_terms_name,
  payment_terms_days
FROM acme_supermarkets.edw_gold.dim_supplier
WHERE is_current = TRUE
LIMIT 10;

-- Payment terms distribution
SELECT 
  payment_terms_name,
  payment_terms_days,
  COUNT(*) AS supplier_count
FROM acme_supermarkets.edw_gold.dim_supplier
WHERE is_current = TRUE
GROUP BY payment_terms_name, payment_terms_days
ORDER BY supplier_count DESC;

