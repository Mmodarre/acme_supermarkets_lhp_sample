-- ============================================================================
-- Create Payment Method Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed payment method dimension with categorization
-- Source: dim_ncr_pmt_method
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_payment_method AS
SELECT
    -- Keys
    pmt_method_key AS payment_method_key,
    method_id AS payment_method_id,
    
    -- Payment method attributes
    code AS payment_code,
    name AS payment_method_name,
    
    -- Categorization (enhance based on actual payment codes)
    CASE 
        WHEN UPPER(code) = 'CASH' THEN 'Cash'
        WHEN UPPER(code) IN ('CREDIT', 'DEBIT') THEN 'Card'
        WHEN UPPER(code) IN ('MOBILE', 'DIGITAL_WALLET', 'PAYPAL', 'APPLE_PAY', 'GOOGLE_PAY') THEN 'Digital Wallet'
        WHEN UPPER(code) IN ('CHECK', 'CHEQUE') THEN 'Check'
        WHEN UPPER(code) = 'GIFT_CARD' THEN 'Gift Card'
        ELSE 'Other'
    END AS payment_type_category,
    
    -- Status
    is_active,
    
    -- Audit
    last_update_dttm,
    
    -- SCD Type 2 fields
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_ncr_pmt_method;

-- Test the view
SELECT 
  payment_method_key,
  payment_code,
  payment_method_name,
  payment_type_category,
  is_active,
  is_current
FROM acme_supermarkets.edw_gold.dim_payment_method
WHERE is_current = TRUE
ORDER BY payment_method_key;

-- Count by payment type
SELECT 
  payment_type_category,
  COUNT(*) AS method_count
FROM acme_supermarkets.edw_gold.dim_payment_method
WHERE is_current = TRUE AND is_active = TRUE
GROUP BY payment_type_category
ORDER BY method_count DESC;

