# 🏪 ACME Supermarkets - Business Metrics Framework

**Version:** 1.0  
**Date:** October 22, 2025  
**Status:** Approved for Implementation  
**Owner:** ACME Leadership Team

---

## 📋 Executive Summary

This document defines ACME Supermarkets' core business metrics framework, designed specifically for our dual-channel retail model (50 physical stores + e-commerce), 4-warehouse fulfillment network, and 4 customer segments (Budget, Premium, Family, Convenience).

### Framework Overview

- **Operational Metrics:** 7 categories covering omnichannel, stores, supply chain, customers, payments, fulfillment, and strategy
- **Financial Metrics:** 3 categories covering profitability, revenue quality, and financial planning
- **Total Metrics:** 100+ specific KPIs
- **Implementation:** Databricks Metric Views on gold layer

---

## 📊 OPERATIONAL METRICS

---

## 1. 🛒 Omnichannel Performance Metrics

### Strategic Context
ACME operates a sophisticated dual-channel model with physical stores (NCR POS) and e-commerce (SFCC). These metrics track how effectively we integrate both channels.

### Key Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Store-to-Online Revenue Ratio** | `POS Revenue / E-commerce Revenue` | 0.65 (current) → 0.75 | Balance channel growth |
| **Cross-Channel Customer Value** | `Revenue from customers buying both channels / Total Revenue * 100` | >30% | Omnichannel engagement |
| **Cross-Channel Customer %** | `Customers buying both channels / Total customers * 100` | >25% | Integration success |
| **Warehouse Fulfillment Coverage** | `E-commerce Orders Fulfilled / Total E-commerce Orders * 100` | >98% | Service level |
| **Regional Channel Mix** | `Channel revenue by geographic regions` | Balanced | Geographic insights |
| **Mobile Readiness Score** | `Customer digital payment % + online customer %` | Baseline for mobile launch | Future preparation |
| **Omnichannel Customer Revenue** | `SUM(revenue WHERE customer uses both channels)` | Track monthly | Core strategy metric |

### Dimensions for Slicing
- Sales Channel (POS, E-commerce, Mobile-future)
- Customer Segment (Budget, Premium, Family, Convenience)
- Geographic Region (West, Other)
- Time Period (Day, Week, Month, Quarter, Year)

---

## 2. 🏬 Store Network Metrics

### Strategic Context
ACME operates 50 stores across multiple regions, with concentration in smaller markets. These metrics evaluate individual and collective store performance.

### Store Productivity Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Store Productivity Index** | `Store Revenue / (Store Employee Count * Days Open)` | Benchmark top quartile | Compare 50 stores |
| **Store-Level EBITDA** | `Store Revenue - (COGS + Store Operating Expenses)` | Positive for all stores | Individual profitability |
| **Revenue per Square Foot** | `Revenue / Total Store Square Footage` | Industry benchmark | Space efficiency |
| **Revenue per Employee** | `Total Revenue / Total Employees` | >$3,200/employee | Labor productivity |
| **Store Labor as % of Revenue** | `Store Labor Costs / Store Revenue * 100` | <15% | Cost control |

### Geographic Performance Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **West Region Market Share** | `West Region Revenue / Total Revenue * 100` | Maintain >5% | Track strongest region |
| **Non-West Expansion Performance** | `Other Region Revenue / Total Revenue * 100` | Grow to >95% | Monitor 47 stores |
| **Small Market Store Performance** | `Revenue for stores in cities <100K population` | Sustainable growth | ACME's niche |
| **Regional Operating Margin** | `(Regional Revenue - Regional Costs) / Regional Revenue * 100` | >10% by region | Regional profitability |
| **NSW/TAS/SA Cluster Performance** | `Revenue from Australian state clusters` | Track trends | International footprint |

### Dimensions for Slicing
- Individual Store (54 locations)
- Store Region (West, Other)
- Store Type (Store, Warehouse)
- Store Size Category
- Market Size (Small, Medium, Large)

---

## 3. 📦 Supply Chain Metrics

### Strategic Context
ACME's 4-warehouse network fulfills e-commerce orders and supplies 50 stores through transfer orders. These metrics optimize the supply chain.

### Warehouse Operations Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Warehouse Transfer Efficiency** | `Transfer Order Fill Rate * On-Time Delivery %` | >95% | Network optimization |
| **Store Replenishment Cycle Time** | `AVG(days from warehouse shipment to store receipt)` | <2 days | Inventory velocity |
| **Warehouse Utilization Rate** | `Warehouse Inventory Value / Warehouse Capacity` | 70-85% | Space optimization |
| **Transfer Cost per Unit** | `Transfer costs / Units transferred` | Minimize | Logistics efficiency |
| **Inter-Location Transfer Cost** | `Total transfer costs / Total units transferred` | Track monthly | Internal logistics |
| **Warehouse Stock Coverage** | `Days of supply at warehouse level` | 14-21 days | Inventory balance |
| **Cross-Dock Success Rate** | `Direct ship orders / Total warehouse receipts * 100` | >20% (if applicable) | Efficiency gain |

### Supplier & Procurement Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Supplier Payment Terms (DPO)** | `(Accounts Payable / COGS) * 365` | >30 days | Cash flow advantage |
| **Purchase Order Accuracy** | `PO Value Matched to Invoice / Total PO Value * 100` | >99% | Procurement quality |
| **Vendor Concentration Risk** | `Top 10 Suppliers Purchases / Total Purchases * 100` | <50% | Supply chain risk |
| **Early Payment Discount Capture** | `Discounts Taken / Discounts Available * 100` | >80% | Cash optimization |

### Inventory Financial Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Inventory Turnover Ratio** | `COGS / Average Inventory Value` | >12x annually | Working capital |
| **Days Inventory Outstanding (DIO)** | `(Average Inventory / COGS) * 365` | <30 days | Inventory efficiency |
| **Stock-to-Sales Ratio** | `End of Period Inventory Value / Period Sales` | <1.5 | Inventory balance |
| **GMROI (Gross Margin Return on Investment)** | `Gross Profit / Average Inventory Cost` | >150% | Profitability per $ |

### Dimensions for Slicing
- Warehouse Location (4 warehouses)
- Destination Store (50 stores)
- Product Category
- Supplier (83 suppliers)
- Transfer Order Type

---

## 4. 👥 Customer Segmentation Metrics

### Strategic Context
ACME serves 4 distinct customer segments: Budget, Premium, Family, and Convenience. Understanding each segment's behavior drives targeting and merchandising.

### Segment Performance Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Budget Segment Revenue** | `SUM(revenue WHERE segment = 'budget')` | Track monthly | Value shoppers |
| **Premium Segment Margin** | `Margin % for premium segment customers` | >10% | High-value customers |
| **Family Segment Basket Size** | `AVG(units per transaction) for family segment` | >15 units | Bulk buyers |
| **Convenience Segment Frequency** | `Transactions per customer for convenience segment` | >2x/week | Quick-trip shoppers |
| **Segment Profitability Index** | `Gross Profit by Segment / Segment Costs` | Rank segments | Resource allocation |
| **Segment-Specific Margin %** | `Margin by Budget/Premium/Family/Convenience` | Varies by segment | Pricing strategy |

### Customer Behavior Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Unique Customers** | `COUNT(DISTINCT customer_key)` | Growing monthly | Customer base |
| **Active Customers (90-day)** | `COUNT(DISTINCT customer_key) with purchase in last 90 days` | >75% of base | Engagement |
| **Repeat Customer Rate** | `Customers with >1 purchase / Total customers * 100` | >60% | Loyalty indicator |
| **Customer Purchase Frequency** | `COUNT(transactions) / COUNT(DISTINCT customers)` | >3x/month | Shopping habits |
| **Segment Migration Rate** | `Customers changing segments / Total customers * 100` | Track trends | Customer evolution |
| **Premium Segment Penetration** | `Premium customers / Total customers * 100` | >20% | Trading-up success |

### Customer Value Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Customer Lifetime Value (CLV)** | `SUM(total_amount) / COUNT(DISTINCT customer_key)` | Track by segment | Long-term value |
| **Revenue per Customer** | `Total Revenue / Unique Customers` | >$25,000 | Customer productivity |
| **Average Order Value (AOV)** | `SUM(total_amount) / COUNT(transactions)` | >$45 | Per-order spending |
| **Customer Acquisition ROI** | `Customer Lifetime Value / Customer Acquisition Cost` | >3:1 | Marketing efficiency |

### Dimensions for Slicing
- Customer Segment (Budget, Premium, Family, Convenience)
- Customer Age Group (18-24, 25-34, 35-44, 45-54, 55-64, 65+)
- Customer Tenure (New, Established, Loyal)
- Geographic Location
- Channel Preference (POS, E-commerce, Both)

---

## 5. 💳 Payment Innovation Metrics

### Strategic Context
ACME offers 16 payment methods including cash, cards, and digital wallets. Tracking payment trends supports our digital transformation and reduces processing costs.

### Payment Method Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Digital Payment Adoption** | `(Mobile + Digital Wallet payments) / Total transactions * 100` | >40% | Cashless transition |
| **Cash Transaction Decline Rate** | `YoY change in cash transaction %` | -10% YoY | Payment evolution |
| **Payment Method Diversity** | `# of payment methods used / # available * 100` | >80% adoption | Offering utilization |
| **Mobile Wallet Revenue Share** | `Mobile payment revenue / Total revenue * 100` | >15% | Mobile readiness |
| **Payment Processing Cost Ratio** | `Total Payment Fees / Revenue * 100` | <2% | Cost efficiency |
| **Cash Transaction Cost** | `Cash handling costs / Cash transactions` | Minimize | Hidden costs |
| **Digital Payment Savings** | `Cash handling costs avoided / Digital transactions` | Track monthly | ROI of innovation |

### Dimensions for Slicing
- Payment Method (16 types)
- Payment Category (Cash, Card, Digital Wallet, Check, Other)
- Sales Channel (POS, E-commerce)
- Customer Segment
- Transaction Size

---

## 6. 🚚 E-commerce Fulfillment Metrics

### Strategic Context
ACME fulfills online orders through 4 warehouses and 6 shipping carriers. These metrics optimize fulfillment speed, cost, and customer satisfaction.

### Fulfillment Performance Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Order-to-Ship Time** | `AVG hours from order to warehouse shipment` | <24 hours | Fulfillment speed |
| **Warehouse Fulfillment Success Rate** | `(On-time orders * Fully fulfilled) / Total orders * 100` | >95% | Service level |
| **Warehouse Allocation Accuracy** | `Orders allocated to nearest warehouse / Total orders * 100` | >85% | Network optimization |
| **Fulfillment Cost Per Order** | `Total fulfillment costs / E-commerce orders` | <$8/order | Unit economics |
| **Multi-Warehouse Order Rate** | `Orders fulfilled from >1 warehouse / Total orders * 100` | <5% | Split shipment cost |
| **Same-Day Fulfillment Capability** | `Orders shipped same day / Total orders * 100` | >20% | Competitive advantage |

### Carrier Performance Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Carrier Performance by Region** | `On-time delivery % by ACME's 6 carriers` | >95% per carrier | Partner management |
| **Carrier Performance Index** | `Weighted score of carriers' on-time delivery` | >90 (0-100 scale) | Overall reliability |
| **Average Delivery Time** | `Days from ship to delivery by carrier` | <3 days | Customer satisfaction |
| **Carrier Cost per Package** | `Shipping costs / Packages shipped by carrier` | Optimize | Cost management |

### Dimensions for Slicing
- Warehouse Location (4 locations)
- Carrier (6 carriers)
- Destination Region
- Order Size
- Product Category
- Customer Segment

---

## 7. 🎯 ACME Strategic KPIs

### Strategic Context
These are ACME's highest-level metrics that leadership monitors to guide strategic decisions. They combine operational and financial elements.

### Top Strategic KPIs

| Metric Name | Formula | Target | Strategic Importance |
|-------------|---------|--------|---------------------|
| **Omnichannel Customer Lifetime Value** | `CLV for customers buying both channels vs single channel` | >2x single channel | Core strategy validation |
| **Warehouse Network ROI** | `E-commerce profit / Warehouse operating costs` | >25% ROI | Justify $MM investment |
| **Segment-Specific Margin** | `Margin % by 4 customer segments` | Optimize mix | Targeting strategy |
| **Transfer Order Efficiency Score** | `(Fill rate * On-time %) / Transfer cost` | Benchmark quarterly | Supply chain excellence |
| **Regional Expansion Success** | `New region performance vs established regions` | Match within 2 years | Growth strategy |
| **Digital Payment Penetration** | `Non-cash transaction %` | >50% | Modernization progress |
| **Health Category Leadership** | `Health product sales vs market` | Market leader | Differentiation |
| **Small Market Dominance** | `Market share in ACME's small-city locations` | >20% local share | Competitive position |

### Dimensions for Slicing
- Time Period (MTD, QTD, YTD)
- Comparison Period (YoY, MoM, WoW)
- Region
- Segment
- Channel

---

## 💰 FINANCIAL METRICS

---

## 8. 📊 Profitability & Margin Analysis

### Strategic Context
Understanding profitability at multiple levels (company, channel, segment, store) enables data-driven decisions about resource allocation and pricing.

### Core Profitability Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Blended Gross Margin %** | `Total Gross Profit / Total Revenue * 100` | 7.63% current → 9% | Overall profitability |
| **Channel Contribution Margin** | `(Channel Revenue - Channel Direct Costs) / Channel Revenue * 100` | POS: 10%, E-com: 6% | Channel profitability |
| **Segment Profitability Index** | `Gross Profit by Segment / Segment Costs` | Premium > Family > Convenience > Budget | Resource allocation |
| **Warehouse-Attributed Margin** | `E-commerce Margin - Warehouse Operating Costs` | >5% | True e-commerce profit |
| **Regional Operating Margin** | `(Regional Revenue - Regional Costs) / Regional Revenue * 100` | >10% all regions | Geographic profitability |
| **Store-Level EBITDA** | `Store Revenue - (COGS + Store Operating Expenses)` | Positive all stores | Individual store health |
| **Category Contribution Margin** | `Category Revenue - (COGS + Direct Category Costs)` | Varies by category | Category management |

### Cost Structure Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Operating Expense Ratio (OER)** | `Total Operating Expenses / Revenue * 100` | <20% | Cost efficiency |
| **Cost-to-Serve by Channel** | `Channel Operating Costs / Channel Revenue * 100` | POS: 12%, E-com: 18% | Channel efficiency |
| **Warehouse Operating Cost Ratio** | `Warehouse Costs / E-commerce Revenue * 100` | <10% | Fulfillment efficiency |
| **Store Labor as % of Revenue** | `Store Labor Costs / Store Revenue * 100` | <15% | Labor productivity |
| **Occupancy Cost Ratio** | `Rent + Utilities / Revenue * 100` | <5% | Real estate efficiency |
| **Shrinkage Cost** | `(Inventory Losses + Waste) / Revenue * 100` | <1.5% | Loss prevention |

### Dimensions for Slicing
- Sales Channel
- Customer Segment
- Store Location
- Product Category
- Time Period

---

## 9. 📈 Revenue Quality & Growth

### Strategic Context
Revenue growth must be sustainable and profitable. These metrics distinguish quality growth from volume-only growth.

### Growth Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Same-Store Sales Growth (SSSG)** | `((Current Period - Prior Period) / Prior Period) * 100` | >3% YoY | Organic growth |
| **Organic Revenue Growth** | `Revenue Growth excluding M&A * 100` | >5% annually | True business growth |
| **Average Transaction Size Growth** | `YoY change in Average Transaction Value` | >2% YoY | Basket growth |
| **Channel Revenue Balance** | `POS Revenue / E-commerce Revenue` | 0.65 → 0.75 | Diversification |
| **New Product Revenue** | `Revenue from products launched in last 12 months / Total Revenue * 100` | >5% | Innovation contribution |

### Revenue Quality Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Revenue per Square Foot** | `Revenue / Total Store Square Footage` | Industry benchmark | Space productivity |
| **Revenue per Employee** | `Total Revenue / Total Employees (1,322)` | >$3,200/employee | Labor productivity |
| **Revenue Concentration Risk** | `Top 10 Customers Revenue / Total Revenue * 100` | <20% | Customer dependency |
| **Recurring Revenue %** | `Subscription/Loyalty Revenue / Total Revenue * 100` | Track trend | Revenue predictability |
| **Revenue per Customer by Segment** | `Segment Revenue / Segment Customers` | Premium highest | Segment value |

### Dimensions for Slicing
- Time Period (Daily, Weekly, Monthly, Quarterly, Yearly)
- Comparison (YoY, MoM, QoQ)
- Geography
- Product Category
- Customer Segment

---

## 10. 🏦 Financial Planning & Analysis

### Strategic Context
These metrics support financial planning, budgeting, and strategic decision-making for ACME's leadership and board.

### Capital Efficiency Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Return on Assets (ROA)** | `Net Income / Total Assets * 100` | >10% | Asset efficiency |
| **Return on Inventory Investment (ROII)** | `Gross Profit / Average Inventory Value * 100` | >25% | Inventory productivity |
| **E-commerce Channel ROI** | `E-commerce Profit / E-commerce Investment * 100` | >20% | Digital transformation ROI |
| **Warehouse Network ROI** | `(E-commerce Profit - Warehouse Costs) / Warehouse Capex * 100` | >25% | Network investment return |
| **Store-Level ROI** | `Store Annual Profit / Store Investment` | >15% | Store investment return |
| **Regional Expansion ROI** | `New Region Profit / Regional Investment` | Payback <3 years | Expansion evaluation |

### Cash Flow Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Cash Conversion Cycle** | `DIO + DSO - DPO` | <30 days | Working capital speed |
| **Days Inventory Outstanding (DIO)** | `(Average Inventory / COGS) * 365` | <25 days | Inventory to cash |
| **Days Sales Outstanding (DSO)** | `(Accounts Receivable / Revenue) * 365` | <5 days | Payment collection |
| **Days Payable Outstanding (DPO)** | `(Accounts Payable / COGS) * 365` | >30 days | Supplier payment terms |
| **Operating Cash Flow per Store** | `Total Operating Cash Flow / 50 stores` | Positive all stores | Store cash generation |
| **Free Cash Flow** | `Operating Cash Flow - Capex` | >$500K annually | Available for growth |

### Planning & Forecasting Metrics

| Metric Name | Formula | Target | Business Value |
|-------------|---------|--------|----------------|
| **Break-Even Point by Channel** | `Fixed Costs / (Revenue per Unit - Variable Cost per Unit)` | Track monthly | Profitability threshold |
| **Store Payback Period** | `Store Investment / Annual Store Cash Flow` | <3 years | New store ROI |
| **Capex as % of Revenue** | `Capital Expenditures / Revenue * 100` | 3-5% | Investment intensity |
| **Budget Variance** | `Actual vs Budget / Budget * 100` | ±5% | Forecasting accuracy |
| **Leverage Ratio** | `Total Debt / EBITDA` | <3x | Financial risk |
| **Interest Coverage Ratio** | `EBITDA / Interest Expense` | >5x | Debt servicing ability |

### Dimensions for Slicing
- Time Period
- Business Unit
- Channel
- Geography
- Scenario (Actual, Budget, Forecast, Best Case, Worst Case)

---

## 🎯 PRIORITIZED METRIC IMPLEMENTATION ROADMAP

### Phase 1: Foundation Metrics (Weeks 1-4)
**Goal:** Establish core business visibility

#### Must-Have Metrics (Deploy First)
1. **Total Revenue** (all channels)
2. **Blended Gross Margin %**
3. **Same-Store Sales Growth**
4. **Omnichannel Customer Revenue**
5. **Warehouse Fulfillment Success Rate**
6. **Channel Contribution Margin**
7. **Unique Customers** (by segment)
8. **Cash Conversion Cycle**

**Rationale:** These provide immediate visibility into ACME's overall health and validate our dual-channel strategy.

---

### Phase 2: Operational Excellence (Weeks 5-8)
**Goal:** Optimize day-to-day operations

#### Operational Metrics
9. **Store Productivity Index**
10. **Transfer Order Efficiency Score**
11. **Carrier Performance Index**
12. **Digital Payment Adoption**
13. **Order-to-Ship Time**
14. **Inventory Turnover Ratio**
15. **Segment-Specific Margin %**

**Rationale:** Drive operational improvements across stores, warehouses, and fulfillment.

---

### Phase 3: Strategic Insights (Weeks 9-12)
**Goal:** Enable strategic decision-making

#### Strategic Metrics
16. **Warehouse Network ROI**
17. **Customer Lifetime Value** (by segment)
18. **Regional Expansion Success**
19. **GMROI (Gross Margin Return on Investment)**
20. **Cross-Channel Customer %**
21. **Revenue per Employee**
22. **Free Cash Flow**

**Rationale:** Support long-term planning and investment decisions.

---

### Phase 4: Advanced Analytics (Weeks 13-16)
**Goal:** Predictive insights and optimization

#### Advanced Metrics
23. **Customer Segmentation Migration**
24. **New Product Revenue Contribution**
25. **Payment Method Cost Optimization**
26. **Multi-Warehouse Order Rate**
27. **Budget Variance Analysis**
28. **All remaining metrics**

**Rationale:** Fine-tune operations and enable predictive analytics.

---

## 📐 METRIC VIEW STRUCTURE

### Proposed Databricks Metric Views

#### Metric View 1: `mv_acme_sales_performance`
```yaml
version: 1.1
source: acme_supermarkets.edw_gold.fact_sales_unified
comment: "Core sales and revenue metrics for ACME Supermarkets"

joins:
  - to: acme_supermarkets.edw_gold.dim_date
    on: fact_sales_unified.date_key = dim_date.date_key
  - to: acme_supermarkets.edw_gold.dim_sales_channel
    on: fact_sales_unified.sales_channel_key = dim_sales_channel.channel_key
  - to: acme_supermarkets.edw_gold.dim_product
    on: fact_sales_unified.product_key = dim_product.product_id AND dim_product.is_current = TRUE
  - to: acme_supermarkets.edw_gold.dim_customer
    on: fact_sales_unified.customer_key = dim_customer.customer_key AND dim_customer.is_current = TRUE
  - to: acme_supermarkets.edw_gold.dim_location
    on: fact_sales_unified.location_key = dim_location.location_id AND dim_location.is_current = TRUE

dimensions:
  - name: Transaction Date
    expr: dim_date.full_date
    
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', dim_date.full_date)
    
  - name: Transaction Quarter
    expr: dim_date.quarter_name
    
  - name: Transaction Year
    expr: dim_date.year
    
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
    
  - name: Channel Type
    expr: dim_sales_channel.channel_type
    
  - name: Customer Segment
    expr: dim_customer.customer_segment
    
  - name: Customer Age Group
    expr: dim_customer.age_group
    
  - name: Product Category
    expr: dim_product.category_name
    
  - name: Product Brand
    expr: dim_product.brand_name
    
  - name: Store Location
    expr: dim_location.location_name
    
  - name: Store Region
    expr: dim_location.region

measures:
  # Revenue Measures
  - name: Total Revenue
    expr: SUM(total_amount)
    format: "$#,##0.00"
    
  - name: Net Revenue
    expr: SUM(subtotal_amount)
    format: "$#,##0.00"
    
  - name: Discount Amount
    expr: SUM(discount_amount)
    format: "$#,##0.00"
    
  # Profitability Measures
  - name: Gross Profit
    expr: SUM(gross_profit)
    format: "$#,##0.00"
    
  - name: Gross Margin %
    expr: SUM(gross_profit) / NULLIF(SUM(total_amount), 0) * 100
    format: "0.00%"
    
  - name: COGS
    expr: SUM(total_cost)
    format: "$#,##0.00"
    
  # Volume Measures
  - name: Units Sold
    expr: SUM(quantity_sold)
    format: "#,##0"
    
  - name: Transaction Count
    expr: COUNT(DISTINCT transaction_number)
    format: "#,##0"
    
  # Customer Measures
  - name: Unique Customers
    expr: COUNT(DISTINCT customer_key)
    format: "#,##0"
    
  - name: Revenue per Customer
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
    format: "$#,##0.00"
    
  - name: Average Transaction Value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT transaction_number), 0)
    format: "$#,##0.00"
    
  - name: Units per Transaction
    expr: SUM(quantity_sold) / NULLIF(COUNT(DISTINCT transaction_number), 0)
    format: "0.00"
```

#### Metric View 2: `mv_acme_channel_performance`
```yaml
version: 1.1
source: acme_supermarkets.edw_gold.fact_sales_unified
comment: "Omnichannel performance metrics specific to ACME's dual-channel model"

dimensions:
  - name: Sales Channel
    expr: dim_sales_channel.channel_name
  - name: Customer Segment
    expr: dim_customer.customer_segment

measures:
  - name: Channel Revenue
    expr: SUM(total_amount)
    
  - name: Channel Margin %
    expr: SUM(gross_profit) / NULLIF(SUM(total_amount), 0) * 100
    
  - name: Store-to-Online Ratio
    expr: SUM(CASE WHEN sales_channel_key = 1 THEN total_amount ELSE 0 END) / 
          NULLIF(SUM(CASE WHEN sales_channel_key = 2 THEN total_amount ELSE 0 END), 0)
```

#### Metric View 3: `mv_acme_customer_analytics`
```yaml
version: 1.1
source: acme_supermarkets.edw_gold.fact_sales_unified
comment: "Customer segmentation and behavior metrics for ACME's 4 segments"

dimensions:
  - name: Customer Segment
    expr: dim_customer.customer_segment
  - name: Customer Age Group
    expr: dim_customer.age_group
  - name: Customer Tenure Group
    expr: CASE 
            WHEN dim_customer.customer_tenure_months < 6 THEN 'New (0-6 months)'
            WHEN dim_customer.customer_tenure_months < 12 THEN 'Growing (6-12 months)'
            ELSE 'Established (12+ months)'
          END

measures:
  - name: Segment Revenue
    expr: SUM(total_amount)
    
  - name: Segment Margin %
    expr: SUM(gross_profit) / NULLIF(SUM(total_amount), 0) * 100
    
  - name: Segment Customers
    expr: COUNT(DISTINCT customer_key)
    
  - name: Segment CLV
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT customer_key), 0)
    
  - name: Segment Basket Size
    expr: SUM(quantity_sold) / NULLIF(COUNT(DISTINCT transaction_number), 0)
```

---

## 📊 METRIC GOVERNANCE

### Metric Ownership

| Metric Category | Owner | Review Frequency |
|----------------|-------|------------------|
| Sales Performance | Chief Revenue Officer | Daily |
| Profitability | Chief Financial Officer | Weekly |
| Omnichannel | Chief Digital Officer | Weekly |
| Store Operations | SVP Retail Operations | Daily |
| Supply Chain | VP Supply Chain | Weekly |
| Customer Analytics | Chief Marketing Officer | Monthly |
| Financial Planning | Chief Financial Officer | Monthly |

### Metric Quality Standards

1. **Accuracy:** All metrics must reconcile to source systems (NCR, SFCC, SAP)
2. **Timeliness:** Core metrics updated daily by 9 AM
3. **Consistency:** Definitions documented and followed across all reports
4. **Accessibility:** Available via Databricks SQL, Power BI, and Tableau
5. **Audit Trail:** All metric calculations logged and version controlled

---

## 🎓 METRIC DEFINITIONS GLOSSARY

### Revenue Terms
- **Gross Sales:** Total before discounts
- **Net Sales:** After discounts, before tax
- **Total Revenue:** Final amount including tax

### Profitability Terms
- **Gross Profit:** Revenue - COGS
- **Gross Margin %:** (Gross Profit / Revenue) * 100
- **Contribution Margin:** Revenue - Variable Costs
- **EBITDA:** Earnings Before Interest, Tax, Depreciation, Amortization

### Customer Terms
- **Active Customer:** Purchase in last 90 days
- **Repeat Customer:** 2+ purchases lifetime
- **CLV (Customer Lifetime Value):** Total expected customer value
- **Segment:** Budget, Premium, Family, or Convenience

### Operational Terms
- **Fill Rate:** Fulfilled qty / Ordered qty
- **On-Time Delivery:** Delivered by expected date
- **Inventory Turnover:** COGS / Average Inventory
- **DIO:** Days Inventory Outstanding

---

## 📅 REPORTING CADENCE

### Daily Reports (9 AM)
- Total Revenue (yesterday vs prior day/year)
- Transaction Count
- Gross Margin %
- Top 10 Store Performance
- Fulfillment Success Rate

### Weekly Reports (Monday 10 AM)
- Week-over-Week Sales Trends
- Channel Performance
- Store Productivity Index
- Inventory Turnover
- Customer Segment Performance

### Monthly Reports (1st Business Day)
- Same-Store Sales Growth
- Financial P&L Metrics
- Customer Analytics Deep Dive
- Warehouse Network Performance
- Strategic KPI Dashboard

### Quarterly Reports (Week 1 of Quarter)
- Comprehensive Business Review
- Financial Planning Metrics
- ROI Analysis
- Strategic Initiative Performance
- Board-Level KPIs

---

## 🔗 RELATED DOCUMENTATION

- **Semantic Layer Technical Documentation:** `/SEMANTIC_LAYER_ANALYSIS_COMPLETE.md`
- **Implementation Guide:** `/SEMANTIC_LAYER_IMPLEMENTATION_COMPLETE.md`
- **Database Schema:** `/docs/Database_schema.md`
- **SQL Scripts:** `/sql/semantic_layer/`

---

## ✅ APPROVAL & SIGN-OFF

| Role | Name | Signature | Date |
|------|------|-----------|------|
| CEO | [Pending] | | |
| CFO | [Pending] | | |
| CRO | [Pending] | | |
| CDO | [Pending] | | |
| Data Team Lead | [Pending] | | |

---

**Document Version:** 1.0  
**Last Updated:** October 22, 2025  
**Next Review:** January 2026  
**Status:** ✅ Ready for Implementation

