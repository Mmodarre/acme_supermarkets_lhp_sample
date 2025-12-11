-- ============================================================================
-- Create Sales Channel Dimension Table
-- ============================================================================
-- Description: Creates sales channel dimension to distinguish between POS,
--              E-commerce, and Mobile sales channels
-- Rows: 3 (POS, E-Commerce, Mobile)
-- ============================================================================

CREATE OR REPLACE TABLE acme_supermarkets.edw_gold.dim_sales_channel (
  channel_key INT NOT NULL COMMENT 'Surrogate key for sales channel',
  channel_id STRING NOT NULL COMMENT 'Natural key/code for channel',
  channel_name STRING NOT NULL COMMENT 'Full name of sales channel',
  channel_type STRING NOT NULL COMMENT 'Physical or Digital',
  channel_category STRING NOT NULL COMMENT 'Retail or Online',
  source_system STRING NOT NULL COMMENT 'Source system (NCR, SFCC)',
  description STRING COMMENT 'Detailed description of channel',
  is_active BOOLEAN COMMENT 'Whether channel is currently active',
  created_date TIMESTAMP COMMENT 'Record creation timestamp'
);

-- Insert channel data
INSERT INTO acme_supermarkets.edw_gold.dim_sales_channel 
(channel_key, channel_id, channel_name, channel_type, channel_category, source_system, description, is_active, created_date) 
VALUES
  (1, 'POS', 'In-Store Point of Sale', 'Physical', 'Retail', 'NCR', 
   'Physical store purchases at POS terminals', TRUE, CURRENT_TIMESTAMP()),
  (2, 'ECOM', 'E-Commerce Website', 'Digital', 'Online', 'SFCC', 
   'Online purchases through the company website', TRUE, CURRENT_TIMESTAMP()),
  (3, 'MOBILE', 'Mobile Application', 'Digital', 'Online', 'SFCC', 
   'Online purchases through mobile app (future channel)', FALSE, CURRENT_TIMESTAMP());

-- Optimize the table
OPTIMIZE acme_supermarkets.edw_gold.dim_sales_channel;

-- Display all rows
SELECT 
  channel_key,
  channel_id,
  channel_name,
  channel_type,
  channel_category,
  source_system,
  is_active
FROM acme_supermarkets.edw_gold.dim_sales_channel
ORDER BY channel_key;

