-- ============================================================================
-- Deploy All Databricks Metric Views
-- ============================================================================
-- Description: Creates 10 metric views for ACME Supermarkets analytics
-- Usage: Execute this entire script in Databricks SQL Editor
-- ============================================================================

USE CATALOG acme_supermarkets;
USE SCHEMA edw_gold;

-- ============================================================================
-- 1. mv_omnichannel_performance
-- ============================================================================

CREATE OR REPLACE VIEW mv_omnichannel_performance
WITH METRICS
LANGUAGE YAML
COMMENT 'Omnichannel performance metrics for ACME Supermarkets'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id

filter: dim_customer.is_current = TRUE AND dim_location.is_current = TRUE

dimensions:
  - name: Transaction Date
    expr: dim_date.full_date
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Quarter
    expr: dim_date.quarter_name
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: Fiscal Year
    expr: dim_date.fiscal_year
  
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  
  - name: Channel Type
    expr: dim_sales_channel.channel_type
  
  - name: Channel Category
    expr: dim_sales_channel.channel_category
  
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Store Region
    expr: dim_location.region
  
  - name: Store Sub-Region
    expr: dim_location.sub_region
  
  - name: Store State
    expr: dim_location.state

measures:
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: POS Revenue
    expr: SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END)
  
  - name: E-commerce Revenue
    expr: SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END)
  
  - name: Store-to-Online Revenue Ratio
    expr: >
      SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END) / 
      NULLIF(SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END), 0)
  
  - name: Cross-Channel Customer Count
    expr: >
      COUNT(DISTINCT CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN customer_key 
      END)
  
  - name: Cross-Channel Customer Percentage
    expr: >
      COUNT(DISTINCT CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN customer_key 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Omnichannel Customer Revenue
    expr: >
      SUM(CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Omnichannel Revenue Percentage
    expr: >
      SUM(CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Unique Customers
    expr: COUNT(DISTINCT customer_key)
  
  - name: Transaction Count
    expr: COUNT(DISTINCT transaction_number)


$$;

SELECT 'mv_omnichannel_performance created successfully' AS status;

-- ============================================================================
-- 2. mv_store_network
-- ============================================================================

CREATE OR REPLACE VIEW mv_store_network
WITH METRICS
LANGUAGE YAML
COMMENT 'Metrics for evaluating individual and collective store performance'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id
  
  - name: dim_employee
    source: acme_supermarkets.edw_gold.dim_employee
    'on': source.employee_key = dim_employee.employee_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key

filter: dim_location.is_current = TRUE AND dim_employee.is_current = TRUE

dimensions:
  - name: Store Location
    expr: dim_location.location_name
  
  - name: Store ID
    expr: dim_location.location_id
  
  - name: Store Type
    expr: dim_location.location_type
  
  - name: Store Region
    expr: dim_location.region
  
  - name: Store Sub-Region
    expr: dim_location.sub_region
  
  - name: Store City
    expr: dim_location.city
  
  - name: Store State
    expr: dim_location.state
  
  - name: Store Country
    expr: dim_location.country
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: Employee Name
    expr: dim_employee.full_name
  
  - name: Employee Role
    expr: dim_employee.employee_role

measures:
  - name: Store Revenue
    expr: SUM(total_amount)
  
  - name: Store Gross Profit
    expr: SUM(gross_profit)
  
  - name: Store Transaction Count
    expr: COUNT(DISTINCT transaction_number)
  
  - name: Store Employee Count
    expr: COUNT(DISTINCT employee_key)
  
  - name: Revenue per Employee
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT employee_key), 0)
  
  - name: Store Productivity Index
    expr: >
      SUM(total_amount) / 
      NULLIF(COUNT(DISTINCT employee_key) * COUNT(DISTINCT dim_date.full_date), 0)
  
  - name: West Region Revenue
    expr: SUM(CASE WHEN dim_location.region = 'West' THEN total_amount ELSE 0 END)
  
  - name: West Region Market Share
    expr: >
      SUM(CASE WHEN dim_location.region = 'West' THEN total_amount ELSE 0 END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Non-West Revenue
    expr: SUM(CASE WHEN dim_location.region != 'West' THEN total_amount ELSE 0 END)
  
  - name: Non-West Expansion Performance
    expr: >
      SUM(CASE WHEN dim_location.region != 'West' THEN total_amount ELSE 0 END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: NSW/TAS/SA Revenue
    expr: >
      SUM(CASE 
        WHEN dim_location.state IN ('NSW', 'TAS', 'SA') 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: NSW/TAS/SA Market Share
    expr: >
      SUM(CASE 
        WHEN dim_location.state IN ('NSW', 'TAS', 'SA') 
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Average Transaction Value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Units Sold
    expr: SUM(quantity_sold)
  
  - name: Gross Margin Percentage
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)


$$;

SELECT 'mv_store_network created successfully' AS status;

-- ============================================================================
-- 3. mv_supply_chain
-- ============================================================================

CREATE OR REPLACE VIEW mv_supply_chain
WITH METRICS
LANGUAGE YAML
COMMENT 'Metrics for optimizing ACME supply chain and warehouse network'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_transfer_orders

joins:
  - name: dim_warehouse
    source: acme_supermarkets.edw_gold.dim_warehouse
    'on': source.source_warehouse_key = dim_warehouse.warehouse_id
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.destination_location_key = dim_location.location_id
  
  - name: dim_product
    source: acme_supermarkets.edw_gold.dim_product
    'on': source.product_key = dim_product.product_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    'on': source.created_date_key = dim_date.date_key

filter: dim_location.is_current = TRUE AND dim_product.is_current = TRUE

dimensions:
  - name: Source Warehouse
    expr: dim_warehouse.warehouse_name
  
  - name: Source Warehouse City
    expr: dim_warehouse.city
  
  - name: Source Warehouse Region
    expr: dim_warehouse.region
  
  - name: Destination Location
    expr: dim_location.location_name
  
  - name: Destination Type
    expr: dim_location.location_type
  
  - name: Destination Region
    expr: dim_location.region
  
  - name: Product Name
    expr: dim_product.product_name
  
  - name: Product Category
    expr: dim_product.category_name
  
  - name: Product Brand
    expr: dim_product.brand_name
  
  - name: Transfer Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transfer Year
    expr: dim_date.year
  
  - name: Transfer Status
    expr: transfer_status

measures:
  - name: Total Transfer Orders
    expr: COUNT(DISTINCT transfer_id)
  
  - name: Total Transfer Lines
    expr: COUNT(*)
  
  - name: Total Units Transferred
    expr: SUM(transfer_quantity)
  
  - name: Transfer Value
    expr: SUM(transfer_value_at_cost)
  
  - name: Average Cycle Time Days
    expr: AVG(total_cycle_time_days)
  
  - name: Store Replenishment Cycle Time
    expr: >
      AVG(CASE 
        WHEN dim_location.location_type = 'Store' 
        THEN total_cycle_time_days 
      END)
  
  - name: On-Time Transfer Count
    expr: COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END)
  
  - name: On-Time Transfer Percentage
    expr: >
      COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(CASE WHEN is_received = TRUE THEN 1 END), 0)
  
  - name: Transfer Fill Rate
    expr: >
      COUNT(CASE WHEN is_completed = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(*), 0)
  
  - name: Warehouse Transfer Efficiency
    expr: >
      (COUNT(CASE WHEN is_completed = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)) * 
      (COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN is_received = TRUE THEN 1 END), 0)) / 
      100.0
  
  - name: Average Units per Transfer
    expr: SUM(transfer_quantity) / NULLIF(COUNT(DISTINCT transfer_id), 0)
  
  - name: Transfer Cost per Unit
    expr: SUM(transfer_value_at_cost) / NULLIF(SUM(transfer_quantity), 0)


$$;

SELECT 'mv_supply_chain created successfully' AS status;

-- ============================================================================
-- 4. mv_customer_segmentation
-- ============================================================================

CREATE OR REPLACE VIEW mv_customer_segmentation
WITH METRICS
LANGUAGE YAML
COMMENT 'Customer Segmentation Metrics - Track 4 customer segments'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key

filter: dim_customer.is_current = TRUE

dimensions:
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Customer Age Group
    expr: dim_customer.age_group
  
  - name: Customer Tenure Group
    expr: >
      CASE 
        WHEN dim_customer.customer_tenure_months < 6 THEN 'New (0-6 months)'
        WHEN dim_customer.customer_tenure_months < 12 THEN 'Growing (6-12 months)'
        ELSE 'Established (12+ months)'
      END
  
  - name: Channel Preference
    expr: dim_sales_channel.channel_name
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Year
    expr: dim_date.year

measures:
  - name: Unique Customers
    expr: COUNT(DISTINCT customer_key)
  
  - name: Active Customers 90 Day
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_date.full_date >= DATE_SUB(CURRENT_DATE(), 90) 
        THEN customer_key 
      END)
  
  - name: Active Customer Rate
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_date.full_date >= DATE_SUB(CURRENT_DATE(), 90) 
        THEN customer_key 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Budget Segment Revenue
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Premium Segment Revenue
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Family Segment Revenue
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Family Segment Basket Size
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN quantity_sold 
        ELSE 0 
      END) / 
      NULLIF(COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN transaction_number 
      END), 0)
  
  - name: Convenience Segment Revenue
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Convenience Segment Frequency
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN transaction_number 
      END) / 
      NULLIF(COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN customer_key 
      END), 0)
  
  - name: Segment Specific Margin Budget
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Segment Specific Margin Premium
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Segment Specific Margin Family
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Segment Specific Margin Convenience
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Customer Purchase Frequency
    expr: >
      COUNT(DISTINCT transaction_number) / 
      NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Premium Segment Penetration
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN customer_key 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Revenue per Customer
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Customer Lifetime Value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)


$$;

SELECT 'mv_customer_segmentation created successfully' AS status;

-- ============================================================================
-- 5. mv_payment_innovation
-- ============================================================================

CREATE OR REPLACE VIEW mv_payment_innovation
WITH METRICS
LANGUAGE YAML
COMMENT 'Metrics for tracking payment trends and digital transformation'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_payment_method
    source: acme_supermarkets.edw_gold.dim_payment_method
    'on': source.payment_method_key = dim_payment_method.payment_method_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key

filter: dim_payment_method.is_current = TRUE AND dim_customer.is_current = TRUE

dimensions:
  - name: Payment Method
    expr: dim_payment_method.payment_method_name
  
  - name: Payment Type Category
    expr: dim_payment_method.payment_type_category
  
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: Transaction Size Bucket
    expr: >
      CASE 
        WHEN total_amount < 25 THEN 'Small (<$25)'
        WHEN total_amount < 50 THEN 'Medium ($25-$50)'
        WHEN total_amount < 100 THEN 'Large ($50-$100)'
        ELSE 'Very Large (>$100)'
      END

measures:
  - name: Total Transactions
    expr: COUNT(DISTINCT transaction_number)
  
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: Digital Payment Transaction Count
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category IN ('Digital Wallet', 'Mobile Payment', 'Card - Debit', 'Card - Credit') 
        THEN transaction_number 
      END)
  
  - name: Cash Transaction Count
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category = 'Cash' 
        THEN transaction_number 
      END)
  
  - name: Digital Payment Adoption
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category IN ('Digital Wallet', 'Mobile Payment', 'Card - Debit', 'Card - Credit') 
        THEN transaction_number 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Cash Transaction Percentage
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category = 'Cash' 
        THEN transaction_number 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Mobile Wallet Transaction Count
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category IN ('Digital Wallet', 'Mobile Payment') 
        THEN transaction_number 
      END)
  
  - name: Mobile Wallet Revenue
    expr: >
      SUM(CASE 
        WHEN dim_payment_method.payment_type_category IN ('Digital Wallet', 'Mobile Payment') 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Mobile Wallet Revenue Share
    expr: >
      SUM(CASE 
        WHEN dim_payment_method.payment_type_category IN ('Digital Wallet', 'Mobile Payment') 
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Payment Method Diversity Count
    expr: COUNT(DISTINCT payment_method_key)
  
  - name: Payment Method Diversity Percentage
    expr: >
      COUNT(DISTINCT payment_method_key) * 100.0 / 
      16.0
  
  - name: Non-Cash Revenue
    expr: >
      SUM(CASE 
        WHEN dim_payment_method.payment_type_category != 'Cash' 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Non-Cash Transaction Percentage
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_payment_method.payment_type_category != 'Cash' 
        THEN transaction_number 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Average Transaction Value by Payment Type
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT transaction_number), 0)


$$;

SELECT 'mv_payment_innovation created successfully' AS status;

-- ============================================================================
-- 6. mv_ecommerce_fulfillment
-- ============================================================================

CREATE OR REPLACE VIEW mv_ecommerce_fulfillment
WITH METRICS
LANGUAGE YAML
COMMENT 'E-commerce Fulfillment Metrics - Track order fulfillment'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_order_fulfillment

joins:
  - name: dim_warehouse
    source: acme_supermarkets.edw_gold.dim_warehouse
    'on': source.warehouse_key = dim_warehouse.warehouse_id
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    'on': source.order_date_key = dim_date.date_key

filter: dim_customer.is_current = TRUE

dimensions:
  - name: Warehouse Location
    expr: dim_warehouse.warehouse_name
  
  - name: Warehouse City
    expr: dim_warehouse.city
  
  - name: Warehouse State
    expr: dim_warehouse.state
  
  - name: Warehouse Region
    expr: dim_warehouse.region
  
  - name: Order Status
    expr: order_status
  
  - name: Fulfillment Status
    expr: fulfillment_status
  
  - name: Is Shipped
    expr: is_shipped
  
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Order Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Order Year
    expr: dim_date.year
  
  - name: Order Day of Week
    expr: dim_date.day_name

measures:
  - name: Total Orders
    expr: COUNT(DISTINCT order_id)
  
  - name: Total Order Value
    expr: SUM(total_amount)
  
  - name: Average Order Value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0)
  
  - name: Order to Ship Hours
    expr: AVG(order_to_ship_hours)
  
  - name: Order to Completion Days
    expr: AVG(order_to_completion_days)
  
  - name: Same Day Fulfillment Count
    expr: COUNT(CASE WHEN is_same_day_fulfillment = TRUE THEN 1 END)
  
  - name: Same Day Fulfillment Capability
    expr: >
      COUNT(CASE WHEN is_same_day_fulfillment = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(DISTINCT order_id), 0)
  
  - name: Next Day Fulfillment Count
    expr: COUNT(CASE WHEN is_next_day_fulfillment = TRUE THEN 1 END)
  
  - name: Next Day Fulfillment Percentage
    expr: >
      COUNT(CASE WHEN is_next_day_fulfillment = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(DISTINCT order_id), 0)
  
  - name: Successfully Fulfilled Orders
    expr: COUNT(CASE WHEN is_successfully_fulfilled = TRUE THEN 1 END)
  
  - name: Warehouse Fulfillment Success Rate
    expr: >
      COUNT(CASE WHEN is_successfully_fulfilled = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(DISTINCT order_id), 0)
  
  - name: On Time Delivery Count
    expr: COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END)
  
  - name: On Time Delivery Rate
    expr: >
      COUNT(CASE WHEN is_on_time_delivery = TRUE THEN 1 END) * 100.0 / 
      NULLIF(COUNT(CASE WHEN is_on_time_delivery IS NOT NULL THEN 1 END), 0)


$$;

SELECT 'mv_ecommerce_fulfillment created successfully' AS status;

-- ============================================================================
-- 7. mv_strategic_kpis
-- ============================================================================

CREATE OR REPLACE VIEW mv_strategic_kpis
WITH METRICS
LANGUAGE YAML
COMMENT 'Strategic KPIs - Top-level metrics for ACME leadership'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id
  
  - name: dim_product
    source: acme_supermarkets.edw_gold.dim_product
    'on': source.product_key = dim_product.product_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]

filter: dim_customer.is_current = TRUE AND dim_location.is_current = TRUE AND dim_product.is_current = TRUE

dimensions:
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  
  - name: Region
    expr: dim_location.region
  
  - name: Product Category
    expr: dim_product.category_name
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Quarter
    expr: dim_date.quarter_name
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: City
    expr: dim_location.city

measures:
  - name: Omnichannel Customer Lifetime Value
    expr: >
      SUM(CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN total_amount 
      END) / 
      NULLIF(COUNT(DISTINCT CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 1
        )
        AND customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          WHERE sales_channel_key = 2
        )
        THEN customer_key 
      END), 0)
  
  - name: Single Channel Customer Lifetime Value
    expr: >
      SUM(CASE 
        WHEN customer_key NOT IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          GROUP BY customer_key 
          HAVING COUNT(DISTINCT sales_channel_key) > 1
        )
        THEN total_amount 
        ELSE 0 
      END) / 
      NULLIF(COUNT(DISTINCT CASE 
        WHEN customer_key NOT IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          GROUP BY customer_key 
          HAVING COUNT(DISTINCT sales_channel_key) > 1
        )
        THEN customer_key 
      END), 0)
  
  - name: Budget Segment Margin
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Premium Segment Margin
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Family Segment Margin
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Convenience Segment Margin
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN gross_profit 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN total_amount 
        ELSE 0 
      END), 0)
  
  - name: Regional Revenue
    expr: SUM(total_amount)
  
  - name: Regional Gross Profit
    expr: SUM(gross_profit)
  
  - name: Regional Expansion Success YoY
    expr: >
      (SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) - 
       SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END)) * 100.0 / 
      NULLIF(SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END), 0)
  
  - name: Digital Payment Penetration
    expr: >
      COUNT(DISTINCT CASE 
        WHEN payment_method_key IS NOT NULL 
        THEN transaction_number 
      END) * 100.0 / 
      NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Health Category Revenue
    expr: >
      SUM(CASE 
        WHEN dim_product.nutrition_data IS NOT NULL 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Health Category Leadership
    expr: >
      SUM(CASE 
        WHEN dim_product.nutrition_data IS NOT NULL 
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: Total Gross Profit
    expr: SUM(gross_profit)
  
  - name: Blended Gross Margin
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)


$$;

SELECT 'mv_strategic_kpis created successfully' AS status;

-- ============================================================================
-- 8. mv_profitability
-- ============================================================================

CREATE OR REPLACE VIEW mv_profitability
WITH METRICS
LANGUAGE YAML
COMMENT 'Profitability metrics at multiple levels'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_product
    source: acme_supermarkets.edw_gold.dim_product
    'on': source.product_key = dim_product.product_id
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id
  
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]

filter: dim_customer.is_current = TRUE AND dim_product.is_current = TRUE AND dim_location.is_current = TRUE

dimensions:
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Product Category
    expr: dim_product.category_name
  
  - name: Product Brand
    expr: dim_product.brand_name
  
  - name: Store Location
    expr: dim_location.location_name
  
  - name: Store Region
    expr: dim_location.region
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Year
    expr: dim_date.year

measures:
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: Total COGS
    expr: SUM(total_cost)
  
  - name: Total Gross Profit
    expr: SUM(gross_profit)
  
  - name: Blended Gross Margin Percentage
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)
  
  - name: POS Channel Revenue
    expr: SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END)
  
  - name: POS Channel Gross Margin
    expr: >
      SUM(CASE WHEN sales_channel_key = 1 THEN gross_profit ELSE 0 END) * 100.0 / 
      NULLIF(SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END), 0)
  
  - name: E-commerce Channel Revenue
    expr: SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END)
  
  - name: E-commerce Channel Gross Margin
    expr: >
      SUM(CASE WHEN sales_channel_key = 2 THEN gross_profit ELSE 0 END) * 100.0 / 
      NULLIF(SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END), 0)
  
  - name: Budget Segment Gross Profit
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'budget' 
        THEN gross_profit 
        ELSE 0 
      END)
  
  - name: Premium Segment Gross Profit
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'premium' 
        THEN gross_profit 
        ELSE 0 
      END)
  
  - name: Family Segment Gross Profit
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'family' 
        THEN gross_profit 
        ELSE 0 
      END)
  
  - name: Convenience Segment Gross Profit
    expr: >
      SUM(CASE 
        WHEN dim_customer.customer_segment = 'convenience' 
        THEN gross_profit 
        ELSE 0 
      END)
  
  - name: Category Gross Margin
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)
  
  - name: Discount Amount
    expr: SUM(discount_amount)
  
  - name: Discount Rate Percentage
    expr: SUM(discount_amount) * 100.0 / NULLIF(SUM(total_amount + discount_amount), 0)
  
  - name: Average Margin per Transaction
    expr: SUM(gross_profit) / NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Gross Profit per Customer
    expr: SUM(gross_profit) / NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Units Sold
    expr: SUM(quantity_sold)
  
  - name: Gross Profit per Unit
    expr: SUM(gross_profit) / NULLIF(SUM(quantity_sold), 0)


$$;

SELECT 'mv_profitability created successfully' AS status;

-- ============================================================================
-- 9. mv_revenue_quality
-- ============================================================================

CREATE OR REPLACE VIEW mv_revenue_quality
WITH METRICS
LANGUAGE YAML
COMMENT 'Metrics for sustainable and profitable revenue growth'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id
  
  - name: dim_product
    source: acme_supermarkets.edw_gold.dim_product
    'on': source.product_key = dim_product.product_id
  
  - name: dim_employee
    source: acme_supermarkets.edw_gold.dim_employee
    'on': source.employee_key = dim_employee.employee_id
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key

filter: dim_location.is_current = TRUE AND dim_customer.is_current = TRUE AND dim_product.is_current = TRUE AND dim_employee.is_current = TRUE

dimensions:
  - name: Transaction Date
    expr: dim_date.full_date
  
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: Store Location
    expr: dim_location.location_name
  
  - name: Customer Segment
    expr: dim_customer.customer_segment
  
  - name: Product Category
    expr: dim_product.category_name
  
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  
  - name: Comparison Period
    expr: >
      CASE 
        WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN 'Current Year'
        WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN 'Prior Year'
        ELSE 'Historical'
      END

measures:
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: Current Year Revenue
    expr: SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END)
  
  - name: Prior Year Revenue
    expr: SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END)
  
  - name: Same Store Sales Growth YoY
    expr: >
      (SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) - 
       SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END)) * 100.0 / 
      NULLIF(SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END), 0)
  
  - name: Organic Revenue Growth
    expr: >
      (SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) - 
       SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END)) * 100.0 / 
      NULLIF(SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END), 0)
  
  - name: Average Transaction Value Current Year
    expr: >
      SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) / 
      NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN transaction_number END), 0)
  
  - name: Average Transaction Value Prior Year
    expr: >
      SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END) / 
      NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN transaction_number END), 0)
  
  - name: Average Transaction Size Growth YoY
    expr: >
      ((SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN transaction_number END), 0)) - 
       (SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN transaction_number END), 0))) * 100.0 / 
      NULLIF((SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END) / 
              NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN transaction_number END), 0)), 0)
  
  - name: Channel Revenue Balance
    expr: >
      SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END) / 
      NULLIF(SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END), 0)
  
  - name: New Product Revenue
    expr: >
      SUM(CASE 
        WHEN dim_product.product_created_at >= DATE_SUB(CURRENT_DATE(), 365) 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: New Product Revenue Percentage
    expr: >
      SUM(CASE 
        WHEN dim_product.product_created_at >= DATE_SUB(CURRENT_DATE(), 365) 
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Revenue per Employee
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT employee_key), 0)
  
  - name: Top 10 Customers Revenue
    expr: >
      SUM(CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          GROUP BY customer_key 
          ORDER BY SUM(total_amount) DESC 
          LIMIT 10
        ) 
        THEN total_amount 
        ELSE 0 
      END)
  
  - name: Revenue Concentration Risk
    expr: >
      SUM(CASE 
        WHEN customer_key IN (
          SELECT customer_key 
          FROM acme_supermarkets.edw_gold.fact_sales_unified 
          GROUP BY customer_key 
          ORDER BY SUM(total_amount) DESC 
          LIMIT 10
        ) 
        THEN total_amount 
        ELSE 0 
      END) * 100.0 / 
      NULLIF(SUM(total_amount), 0)
  
  - name: Revenue per Customer by Segment
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Transaction Count
    expr: COUNT(DISTINCT transaction_number)
  
  - name: Transaction Count Growth YoY
    expr: >
      (COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN transaction_number END) - 
       COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN transaction_number END)) * 100.0 / 
      NULLIF(COUNT(DISTINCT CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN transaction_number END), 0)


$$;

SELECT 'mv_revenue_quality created successfully' AS status;

-- ============================================================================
-- 10. mv_financial_planning
-- ============================================================================

CREATE OR REPLACE VIEW mv_financial_planning
WITH METRICS
LANGUAGE YAML
COMMENT 'Metrics for financial planning and strategic decision-making'
AS $$
version: 0.1

source: acme_supermarkets.edw_gold.fact_sales_unified

joins:
  - name: dim_date
    source: acme_supermarkets.edw_gold.dim_date
    using: [date_key]
  
  - name: dim_sales_channel
    source: acme_supermarkets.edw_gold.dim_sales_channel
    'on': source.sales_channel_key = dim_sales_channel.channel_key
  
  - name: dim_location
    source: acme_supermarkets.edw_gold.dim_location
    'on': source.location_key = dim_location.location_id
  
  - name: dim_customer
    source: acme_supermarkets.edw_gold.dim_customer
    'on': source.customer_key = dim_customer.customer_id

filter: dim_location.is_current = TRUE AND dim_customer.is_current = TRUE

dimensions:
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
  
  - name: Transaction Quarter
    expr: dim_date.quarter_name
  
  - name: Transaction Year
    expr: dim_date.year
  
  - name: Fiscal Year
    expr: dim_date.fiscal_year
  
  - name: Channel
    expr: dim_sales_channel.channel_name
  
  - name: Geography
    expr: dim_location.region
  
  - name: Customer Segment
    expr: dim_customer.customer_segment

measures:
  - name: Total Revenue
    expr: SUM(total_amount)
  
  - name: Total Gross Profit
    expr: SUM(gross_profit)
  
  - name: Total COGS
    expr: SUM(total_cost)
  
  - name: E-commerce Profit
    expr: SUM(CASE WHEN sales_channel_key = 2 THEN gross_profit ELSE 0 END)
  
  - name: E-commerce Revenue
    expr: SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END)
  
  - name: Revenue Growth Rate
    expr: >
      (SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) THEN total_amount ELSE 0 END) - 
       SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END)) * 100.0 / 
      NULLIF(SUM(CASE WHEN dim_date.year = YEAR(CURRENT_DATE()) - 1 THEN total_amount ELSE 0 END), 0)
  
  - name: Gross Margin Percentage
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)
  
  - name: Gross Margin Trend
    expr: SUM(gross_profit) * 100.0 / NULLIF(SUM(total_amount), 0)
  
  - name: New Customers
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_tenure_months <= 1 
        THEN customer_key 
      END)
  
  - name: Customer Acquisition Trend
    expr: >
      COUNT(DISTINCT CASE 
        WHEN dim_customer.customer_tenure_months <= 1 
        THEN customer_key 
      END)
  
  - name: Revenue per Customer
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Revenue per Customer Trend
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
  
  - name: Units Sold
    expr: SUM(quantity_sold)
  
  - name: Average Transaction Value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT transaction_number), 0)
  
  - name: Transaction Count
    expr: COUNT(DISTINCT transaction_number)
  
  - name: Discount Amount
    expr: SUM(discount_amount)
  
  - name: Discount Percentage
    expr: SUM(discount_amount) * 100.0 / NULLIF(SUM(total_amount + discount_amount), 0)
  
  - name: POS Channel Revenue
    expr: SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END)
  
  - name: POS Channel Gross Profit
    expr: SUM(CASE WHEN sales_channel_key = 1 THEN gross_profit ELSE 0 END)


$$;

SELECT 'mv_financial_planning created successfully' AS status;

-- ============================================================================
-- Verification
-- ============================================================================

SHOW VIEWS IN acme_supermarkets.edw_gold LIKE 'mv_%';

SELECT 'All metric views deployed successfully!' AS final_status;
