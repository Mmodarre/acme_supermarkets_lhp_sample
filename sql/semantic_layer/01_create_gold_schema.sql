-- ============================================================================
-- Create Gold Layer Schema
-- ============================================================================
-- Description: Creates the gold layer (semantic layer) schema for business-
--              friendly dimensions and facts
-- Author: Semantic Layer Implementation
-- Date: 2025-10-22
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS acme_supermarkets.edw_gold
COMMENT 'Gold layer - Semantic layer with business-friendly dimensions and facts for analytics';

-- Verify schema was created
SHOW SCHEMAS IN acme_supermarkets LIKE 'edw_gold';

