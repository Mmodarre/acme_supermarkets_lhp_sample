### Define Metric View Using Another Metric View as Source

Source: https://docs.databricks.com/aws/en/metric-views/yaml-ref

This YAML configuration demonstrates how to create a new metric view by leveraging an existing metric view as its data source. It shows how to reference dimensions and measures from the source metric view and define new ones.

```yaml
version: 0.1
source: views.examples.source_metric_view

dimensions:
  # Dimension referencing dimension from source_metric_view
  - name: Order date
    expr: order_date_dim

measures:
  # Measure referencing dimension from source_metric_view
  - name: Latest order month
    expr: MAX(order_date_dim_month)

  # Measure referencing measure from source_metric_view
  - name: Latest order year
    expr: DATE_TRUNC('year', MEASURE(max_order_date_measure))

```

--------------------------------

### Describe Metric View Definition as JSON (SQL)

Source: https://docs.databricks.com/aws/en/metric-views

This SQL query retrieves the complete YAML definition of a metric view, including its measures, dimensions, joins, and semantic metadata. The output is formatted as JSON, with semantic metadata stored in the 'metadata' field of each column. This is useful for programmatic access and detailed inspection of metric view configurations.

```sql
DESCRIBE TABLE EXTENDED <catalog.schema.metric_view_name> AS JSON  
```

--------------------------------

### Databricks SQL: Create Metric View with YAML

Source: https://docs.databricks.com/gcp/pt/sql/language-manual/sql-ref-syntax-ddl-create-view

Creates a metric view defined by a YAML specification. Metric views are used for defining aggregated measures and dimensions, allowing for optimized querying. The `LANGUAGE YAML` clause is required, and the view body must contain valid YAML.

```sql
CREATE VIEW view_name LANGUAGE YAML AS $$ yaml_string $$
```

--------------------------------

### Use Metric View as Source in Databricks

Source: https://docs.databricks.com/gcp/en/metric-views/yaml-ref

Demonstrates how to define a new metric view using an existing metric view as its data source. It shows how to reference dimensions and measures from the source view.

```yaml
version: 0.1
source: views.examples.source_metric_view

dimensions:
  
  # Dimension referencing dimension from source_metric_view  
  - name: Order date
    expr: order_date_dim
  
measures:
  
  # Measure referencing dimension from source_metric_view  
  - name: Latest order month
    expr: MAX(order_date_dim_month)
  
  # Measure referencing measure from source_metric_view  
  - name: Latest order year
    expr: DATE_TRUNC('year', MEASURE(max_order_date_measure))
  

```

--------------------------------

### Create Metric View in Databricks SQL (YAML)

Source: https://docs.databricks.com/gcp/ja/sql/language-manual/sql-ref-syntax-ddl-create-view

Illustrates the creation of a metric view using a YAML specification. Metric views are used for analytics and define dimensions and measures.

```sql
CREATE VIEW metric_view_name
METRICS
LANGUAGE YAML
AS $$
yaml_string_specification
$$;
```

--------------------------------

### Unity Catalog metric views (Public Preview)

Source: https://docs.databricks.com/aws/pt/ai-bi/release-notes/2025

Introduces metric views in Unity Catalog for the public preview, enabling centralized definition and management of reusable business metrics.

```APIDOC
## POST /api/metricviews/create

### Description
Creates a new metric view in Unity Catalog.

### Method
POST

### Endpoint
/api/metricviews/create

### Parameters
#### Request Body
- **name** (string) - Required - The name of the metric view.
- **description** (string) - Optional - A description for the metric view.
- **sql** (string) - Required - The SQL query defining the metric view.
- **catalog** (string) - Required - The Unity Catalog name.
- **schema** (string) - Required - The schema name.

### Request Example
{
  "name": "TotalRevenue",
  "description": "Calculates the total revenue.",
  "sql": "SELECT SUM(amount) FROM sales_transactions",
  "catalog": "main",
  "schema": "finance"
}

### Response
#### Success Response (200)
- **metricViewId** (string) - The ID of the created metric view.
- **message** (string) - A confirmation message.

#### Response Example
{
  "metricViewId": "mv_abc789",
  "message": "Metric view created successfully."
}
```

--------------------------------

### Metric Views in Public Preview

Source: https://docs.databricks.com/gcp/en/ai-bi/release-notes/2025

Unity Catalog metric views are now in Public Preview, offering a centralized way to define and manage business metrics.

```APIDOC
## Unity Catalog Metric Views (Public Preview)

### Description
Metric views provide a centralized method for defining and managing consistent, reusable, and governed business metrics. They abstract complex business logic, enabling consistent use of key performance indicators across reporting tools.

### Method
N/A (Feature description)

### Endpoint
N/A (Feature description)

### Parameters
N/A

### Request Example
N/A

### Response
N/A
```

--------------------------------

### Define Measures in Databricks Metric Views

Source: https://docs.databricks.com/gcp/en/metric-views/yaml-ref

Shows how to define measures in Databricks metric views using aggregate SQL expressions. Examples include basic aggregation, ratios, and measure-level filters.

```yaml
measures:
  
  # Basic aggregation
  - name: Total revenue
    expr: SUM(o_totalprice)
  
  # Basic aggregation with ratio
  - name: Total revenue per customer
    expr: SUM(`Total revenue`) / COUNT(DISTINCT o_custkey)
  
  # Measure-level filter
  - name: Total revenue for open orders
    expr: COUNT(o_totalprice) FILTER (WHERE o_orderstatus='O')
  
  # Measure-level filter with multiple aggregate functions
  # filter needs to be specified for each aggregate function in the expression
  - name: Total revenue per customer for open orders
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')/COUNT(DISTINCT o_custkey) FILTER (WHERE o_orderstatus='O')
  

```

--------------------------------

### Metric View Definition using YAML in Databricks

Source: https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-view

Shows the syntax for creating a metric view in Databricks SQL and Databricks Runtime (16.4+). Metric views are defined using a YAML specification that outlines dimensions and measures, and they require the LANGUAGE YAML clause.

```sql
CREATE VIEW view_name
LANGUAGE YAML
AS $$ yaml_string $$
```

--------------------------------

### Create a Metric View with YAML in Databricks SQL

Source: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view

This example shows how to define a metric view using YAML. Metric views are used for defining aggregations and dimensions, simplifying analytical queries.

```sql
CREATE VIEW sales_metrics
LANGUAGE YAML
AS $$ 
metric: "total_sales"
object: "sales"
dimensions:
  - "product_id"
  - "region"
measures:
  - "sum(amount)" 
$$;
```

--------------------------------

### Create Metric View with YAML in Databricks SQL

Source: https://docs.databricks.com/aws/ja/sql/language-manual/sql-ref-syntax-ddl-create-view

This snippet shows how to create a metric view in Databricks SQL using a YAML specification. Metric views are designed for analytics and define dimensions and measures. The view must be defined with LANGUAGE YAML, and the body must be a valid YAML specification.

```sql
CREATE VIEW view_name AS $$ yaml_string $$
```

--------------------------------

### Create Databricks Metric View with YAML Definition

Source: https://docs.databricks.com/aws/ja/sql/language-manual/sql-ref-syntax-ddl-create-view

This example illustrates the creation of a metric view in Databricks SQL using a YAML definition. Metric views are designed for analytical purposes and can define dimensions and measures based on source data, allowing for complex aggregations and calculations. The `DESCRIBE EXTENDED` command shows the detailed schema and metadata of the created metric view.

```sql
-- Creates a Metric View as specified in the YAML definition, with three dimensions and four measures representing the count of orders.  
> CREATE OR REPLACE VIEW region_sales_metrics  
  (month COMMENT 'Month order was made',  
   status,  
   order_priority,  
   count_orders COMMENT 'Count of orders',  
   total_Revenue,  
   total_revenue_per_customer,  
   total_revenue_for_open_orders)  
  WITH METRICS  
  LANGUAGE YAML  
  COMMENT 'A Metric View for regional sales metrics.'  
  AS $$  
   version: 0.1  
   source: samples.tpch.orders  
   filter: o_orderdate > '1990-01-01'  
   dimensions:  
   - name: month  
     expr: date_trunc('MONTH', o_orderdate)  
   - name: status  
     expr: case  
       when o_orderstatus = 'O' then 'Open'  
       when o_orderstatus = 'P' then 'Processing'  
       when o_orderstatus = 'F' then 'Fulfilled'  
       end  
   - name: prder_priority  
     expr: split(o_orderpriority, '-')[1]  
   measures:  
   - name: count_orders  
     expr: count(1)  
   - name: total_revenue  
     expr: SUM(o_totalprice)  
   - name: total_revenue_per_customer  
     expr: SUM(o_totalprice) / count(distinct o_custkey)  
   - name: total_revenue_for_open_orders  
     expr: SUM(o_totalprice) filter (where o_orderstatus='O')  
  $$;  
  
> DESCRIBE EXTENDED region_sales_metrics;  
  col_name                       data_type  
  ------------------------------ --------------------------  
  month                          timestamp  
  status                         string  
  order_priority                 string  
  count_orders                   bigint measure  
  total_revenue                  decimal(28,2) measure  
  total_revenue_per_customer     decimal(38,12) measure  
  total_revenue_for_open_orders  decimal(28,2) measure  
  
  # Detailed Table Information  
  Catalog                        main  
  Database                       default  
  Table                          region_sales_metrics  
  Owner                          alf@melmak.et  
  Created Time                   Thu May 15 13:03:01 UTC 2025  
  Last Access                    UNKNOWN  
  Created By                     Spark  
  Type                           METRIC_VIEW  
  Comment                        A Metric View for regional sales metrics.  
  Use Remote Filtering           false  

```

--------------------------------

### Databricks SQL: Alter Metric View Definition

Source: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-view

Provides an example of altering a metric view in Databricks SQL, specifically to drop a measure. The example shows the use of ALTER VIEW with a new definition for the metric view, including dimensions and measures.

```sql
-- Alter a the metric view `region_sales_metrics` defined in CREATE VIEW to drop the `total_revenue_for_open_orders` measure.  
> ALTER VIEW region_sales_metrics  
  AS $$  
   version: 0.1  
   source: samples.tpch.orders  
   filter: o_orderdate > '1990-01-01'  
   dimensions:  
   - name: month  
     expr: date_trunc('MONTH', o_orderdate)  
   - name: status  
     expr: case  
       when o_orderstatus = 'O' then 'Open'  
       when o_orderstatus = 'P' then 'Processing'  
       when o_orderstatus = 'F' then 'Fulfilled'  
       end  
   - name: order_priority  
     expr: split(o_orderpriority, '-')[1]  
   measures:  
   - name: count_orders  
     expr: count(1)  
   - name: total_revenue  
     expr: SUM(o_totalprice)  
   - name: total_revenue_per_customer  
     expr: SUM(o_totalprice) / count(distinct o_custkey)  
  $$;  
  
> DESCRIBE EXTENDED region_sales_metrics;  
 col_name                    data_type  
 month	                     timestamp  
 status	                     string  
 prder_priority              string  
 count_orders                bigint measure  
 total_revenue               decimal(28,2) measure  
 total_revenue_per_customer  decimal(38,12) measure  
  
 # Detailed Table Information  
 Catalog                     main  
 Database                    default  
 Table                       region_sales_metrics  
 Owner                       alf@melmak.et  
 Created Time                Sun May 18 23:45:25 UTC 2025  
 Last Access                 UNKNOWN  
 Created By                  Spark  
 Type                        METRIC_VIEW  
 Comment                     A metric view for regional sales metrics.  
 View Text                   "  
    version: 0.1  

```

--------------------------------

### Define Unity Catalog Metric View in YAML

Source: https://docs.databricks.com/gcp/pt/metric-views

This YAML snippet demonstrates the structure for defining a Unity Catalog metric view. It specifies data sources, filters, dimensions (categorical attributes), and measures (aggregated business values). The 'source' can be a table or SQL query, and dimensions/measures are defined with expressions.

```yaml
version: 1.1  
  
source: samples.tpch.orders  
filter: o_orderdate > '1990-01-01'  
  
dimensions:  
  - name: Order Month  
    expr: DATE_TRUNC('MONTH', o_orderdate)  
  
  - name: Order Status  
    expr: CASE  
      WHEN o_orderstatus = 'O' then 'Open'  
      WHEN o_orderstatus = 'P' then 'Processing'  
      WHEN o_orderstatus = 'F' then 'Fulfilled'  
      END  
  
  - name: Order Priority  
    expr: SPLIT(o_orderpriority, '-')[1]  
  
measures:  
  - name: Order Count  
    expr: COUNT(1)  
  
  - name: Total Revenue  
    expr: SUM(o_totalprice)  
  
  - name: Total Revenue per Customer  
    expr: SUM(o_totalprice) / COUNT(DISTINCT o_custkey)  
  
  - name: Total Revenue for Open Orders  
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')
```

--------------------------------

### Create Metric View Syntax (YAML)

Source: https://docs.databricks.com/aws/pt/sql/language-manual/sql-ref-syntax-ddl-create-view

Provides the syntax for creating a metric view in Databricks SQL using a YAML specification. Metric views are used for defining measures and dimensions for aggregation.

```sql
CREATE VIEW view_name
LANGUAGE YAML
AS $$ 
metrics:
  dimensions:
    - column_name: "date"
      type: "timestamp"
  measures:
    - name: "total_sales"
      agg: "sum"
      column: "sales"
$$;
```

--------------------------------

### Define Dimensions in Databricks Metric Views

Source: https://docs.databricks.com/gcp/en/metric-views/yaml-ref

Illustrates the definition of dimensions within a Databricks metric view. It covers simple column references, SQL expressions, handling column names with spaces, and multi-line expressions.

```yaml
dimensions:
  
  # Column name
  - name: Order date
    expr: o_orderdate
  
  # SQL expression
  - name: Order month
    expr: DATE_TRUNC('MONTH', `Order date`)
  
  # Referring to a column with a space in the name
  - name: Month of order
    expr: `Order month`
  
  # Multi-line expression
  - name: Order status
    expr: CASE
            WHEN o_orderstatus = 'O' THEN 'Open'
            WHEN o_orderstatus = 'P' THEN 'Processing'
            WHEN o_orderstatus = 'F' THEN 'Fulfilled'
          END
  

```

--------------------------------

### Define Databricks Metric View using YAML

Source: https://docs.databricks.com/aws/en/metric-views/create

This YAML definition outlines the structure for a Databricks metric view. It specifies the data source, filtering conditions, dimensions for analysis, and measures for key performance indicators, including aggregation expressions and conditional logic.

```yaml
version: 0.1  
  
source: samples.tpch.orders  
filter: o_orderdate > '1990-01-01'  
  
dimensions:  
  - name: Order Month  
    expr: DATE_TRUNC('MONTH', o_orderdate)  
  
  - name: Order Status  
    expr: CASE  
      WHEN o_orderstatus = 'O' then 'Open'  
      WHEN o_orderstatus = 'P' then 'Processing'  
      WHEN o_orderstatus = 'F' then 'Fulfilled'  
      END  
  
  - name: Order Priority  
    expr: SPLIT(o_orderpriority, '-')[1]  
  
measures:  
  - name: Order Count  
    expr: COUNT(1)  
  
  - name: Total Revenue  
    expr: SUM(o_totalprice)  
  
  - name: Total Revenue per Customer  
    expr: SUM(o_totalprice) / COUNT(DISTINCT o_custkey)  
  
  - name: Total Revenue for Open Orders  
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')  


```

--------------------------------

### Define Unity Catalog Metric View in YAML

Source: https://docs.databricks.com/aws/en/metric-views

This YAML configuration defines a metric view for order data. It specifies a source table, filters, dimensions (Order Month, Order Status, Order Priority), and measures (Order Count, Total Revenue, Total Revenue per Customer, Total Revenue for Open Orders).

```yaml
version: 1.1  
  
source: samples.tpch.orders  
filter: o_orderdate > '1990-01-01'  
  
dimensions:  
  - name: Order Month  
    expr: DATE_TRUNC('MONTH', o_orderdate)  
  
  - name: Order Status  
    expr: CASE  
      WHEN o_orderstatus = 'O' then 'Open'  
      WHEN o_orderstatus = 'P' then 'Processing'  
      WHEN o_orderstatus = 'F' then 'Fulfilled'  
      END  
  
  - name: Order Priority  
    expr: SPLIT(o_orderpriority, '-')[1]  
  
measures:  
  - name: Order Count  
    expr: COUNT(1)  
  
  - name: Total Revenue  
    expr: SUM(o_totalprice)  
  
  - name: Total Revenue per Customer  
    expr: SUM(o_totalprice) / COUNT(DISTINCT o_custkey)  
  
  - name: Total Revenue for Open Orders  
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')  

```

--------------------------------

### Define Unity Catalog Metric View in YAML

Source: https://docs.databricks.com/gcp/en/metric-views

This snippet demonstrates the YAML structure for defining a Unity Catalog metric view. It includes source data, filters, dimensions (e.g., Order Month, Order Status), and measures (e.g., Order Count, Total Revenue). This YAML definition is registered in Unity Catalog.

```yaml
version: 1.1  
  
source: samples.tpch.orders  
filter: o_orderdate > '1990-01-01'  
  
dimensions:  
  - name: Order Month  
    expr: DATE_TRUNC('MONTH', o_orderdate)  
  
  - name: Order Status  
    expr: CASE  
      WHEN o_orderstatus = 'O' then 'Open'  
      WHEN o_orderstatus = 'P' then 'Processing'  
      WHEN o_orderstatus = 'F' then 'Fulfilled'  
      END  
  
  - name: Order Priority  
    expr: SPLIT(o_orderpriority, '-')[1]  
  
measures:  
  - name: Order Count  
    expr: COUNT(1)  
  
  - name: Total Revenue  
    expr: SUM(o_totalprice)  
  
  - name: Total Revenue per Customer  
    expr: SUM(o_totalprice) / COUNT(DISTINCT o_custkey)  
  
  - name: Total Revenue for Open Orders  
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')  


```

--------------------------------

### Query a Metric View using SQL

Source: https://docs.databricks.com/aws/en/metric-views/create

This SQL query demonstrates how to select and aggregate measures from a metric view named 'orders_metric_view'. It requires Databricks Runtime 16.4 or above and measures must be wrapped in the MEASURE function. The query groups results by 'Order Month' and 'Order Status', and sorts them by 'Order Month'.

```sql
SELECT  
 `Order Month`,  
 `Order Status`,  
 MEASURE(`Order Count`),  
 MEASURE(`Total Revenue`),  
 MEASURE(`Total Revenue per Customer`)  
FROM  
 orders_metric_view  
GROUP BY ALL  
ORDER BY 1 ASC  

```

--------------------------------

### Define Measures for a Metric View

Source: https://docs.databricks.com/aws/en/metric-views/yaml-ref

This YAML configuration demonstrates how to define measures in a Databricks metric view. It includes examples of basic aggregations (SUM, COUNT DISTINCT), calculating ratios, and applying measure-level filters using the FILTER clause.

```yaml
measures:
  # Basic aggregation
  - name: Total revenue
    expr: SUM(o_totalprice)

  # Basic aggregation with ratio
  - name: Total revenue per customer
    expr: SUM(`Total revenue`) / COUNT(DISTINCT o_custkey)

  # Measure-level filter
  - name: Total revenue for open orders
    expr: COUNT(o_totalprice) FILTER (WHERE o_orderstatus='O')

  # Measure-level filter with multiple aggregate functions
  # filter needs to be specified for each aggregate function in the expression
  - name: Total revenue per customer for open orders
    expr: SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')/COUNT(DISTINCT o_custkey) FILTER (WHERE o_orderstatus='O')

```

--------------------------------

### Define display names in Databricks metric view YAML

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

Demonstrates how to define human-readable display names for dimensions and measures in a Databricks metric view's YAML definition. This improves how data appears in visualization tools.

```yaml
version: 1.1
source: samples.tpch.orders

dimensions:
  - name: order_date
    expr: o_orderdate
    display_name: 'Order Date'

measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    display_name: 'Total Revenue'

```

--------------------------------

### YAML Complete Metric View Definition Example

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

This comprehensive YAML example defines a metric view in Databricks. It includes dimensions like 'order_date' with date formatting and measures like 'total_revenue' with currency formatting, showcasing various semantic metadata types.

```YAML
version: 1.1
source: samples.tpch.orders
comment: Comprehensive sales metrics with enhanced semantic metadata
dimensions:
  - name: order_date
    expr: o_orderdate
    comment: Date when the order was placed
    display_name: Order Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: true
    synonyms:
      - order time
      - date of order
  - name: customer_segment
    expr: |
      CASE
        WHEN o_totalprice > 100000 THEN 'Enterprise'
        WHEN o_totalprice > 10000 THEN 'Mid-market'
        ELSE 'SMB'
      END
    comment: Customer classification based on order value
    display_name: Customer Segment
    synonyms:
      - segment
      - customer tier
measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    comment: Total revenue from all orders
    display_name: Total Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
    synonyms:
      - revenue
      - total sales
      - sales amount
  - name: order_count
    expr: COUNT(1)
    comment: Total number of orders
    display_name: Order Count
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true
    synonyms:
      - count
      - number of orders
  - name: avg_order_value
    expr: SUM(o_totalprice) / COUNT(1)
    comment: Average revenue per order
    display_name: Average Order Value
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - aov
      - average revenue

```

--------------------------------

### Configure number format in Databricks metric view YAML

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

Example of configuring numeric formatting for a metric view in Databricks YAML. It specifies maximum decimal places, disabling group separators, and using compact abbreviation.

```yaml
format:
  type: number
  decimal_places:
    type: max
    places: 2
  hide_group_separator: false
  abbreviation: compact

```

--------------------------------

### Querying Window Measure with SQL

Source: https://docs.databricks.com/aws/en/metric-views/window-measures

Demonstrates how to query a metric view that includes a window measure using SQL. This example selects state, truncated date, and the result of a window measure (t7d_distinct_customers) from a sales metric view.

```SQL
SELECT  
   state,  
   DATE_TRUNC('month', date),  
   MEASURE(t7d_distinct_customers) as m  
FROM sales_metric_view  
WHERE date >= DATE'2024-06-01'  
GROUP BY ALL  

```

--------------------------------

### Configure percentage format in Databricks metric view YAML

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

Provides a YAML example for configuring percentage formatting in a Databricks metric view. It sets the format type to percentage and enables hiding group separators.

```yaml
format:
  type: percentage
  decimal_places:
    type: all
  hide_group_separator: true

```

--------------------------------

### SQL: Create Temporary Metric View

Source: https://docs.databricks.com/aws/pt/release-notes/runtime/17

This SQL snippet demonstrates the use of the `TEMPORARY` keyword when creating a metric view. Temporary metric views are session-specific and are automatically dropped when the session ends.

```sql
CREATE TEMPORARY VIEW my_temp_view AS
SELECT column1, column2 FROM my_table;
```

--------------------------------

### Add synonyms to Databricks metric view YAML

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

Shows how to define synonyms for dimensions and measures in a Databricks metric view YAML. Synonyms help LLM tools discover data through alternative names, supporting both block and flow style definitions.

```yaml
version: 1.1
source: samples.tpch.orders

dimensions:
  - name: order_date
    expr: o_orderdate
    # block style
    synonyms:
      - 'order time'
      - 'date of order'

measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    # flow style
    synonyms: ['revenue', 'total sales']

```

--------------------------------

### Temporary Metric Views

Source: https://docs.databricks.com/aws/pt/release-notes/runtime/17

Introduces support for the TEMPORARY keyword during metric view creation, making views session-scoped.

```APIDOC
## Temporary Metric View Creation

### Description
Allows the use of the `TEMPORARY` keyword when creating a metric view. Temporary metric views are only visible within the session that created them and are automatically dropped when the session ends.

### Method
SQL Statement

### Endpoint
N/A (SQL Statement)

### Parameters
#### Path Parameters
None

#### Query Parameters
None

#### Request Body
None

### Request Example
```sql
CREATE TEMPORARY VIEW my_temporary_view AS
SELECT column1, column2 FROM my_table;
```

### Response
#### Success Response (200)
- **message** (string) - Confirmation of view creation.

#### Response Example
```json
{
  "message": "Temporary view 'my_temporary_view' created successfully."
}
```
```

--------------------------------

### Example SQL query referencing a metric view

Source: https://docs.databricks.com/gcp/en/dashboards/datasets

This SQL query shows how to reference a metric view within a dataset definition. It utilizes the MEASURE aggregate function to access metric view measures and allows for filtering or reshaping the data. Ensure all measures are accessed using MEASURE.

```sql
SELECT
  MEASURE(your_metric_view.your_measure),
  dimension1,
  dimension2
FROM
  your_metric_view
WHERE
  dimension1 = 'filter_value';
```

--------------------------------

### Define Synonyms in Metric View YAML (Block and Flow Style)

Source: https://docs.databricks.com/aws/en/metric-views/semantic-metadata

This YAML snippet illustrates how to define synonyms for dimensions and measures in a Databricks metric view using both block and flow styles. Synonyms aid LLM tools in discovering data by providing alternative names. Each dimension/measure can have up to 10 synonyms, each limited to 255 characters. Supports Databricks Runtime 17.2+ and spec version 1.1+.

```yaml
version: 1.1  
source: samples.tpch.orders  
  
dimensions:  
  - name: order_date  
    expr: o_orderdate  
    # block style  
    synonyms:  
      - 'order time'  
      - 'date of order'  
  
measures:  
  - name: total_revenue  
    expr: SUM(o_totalprice)  
    # flow style  
    synonyms: ['revenue', 'total sales']  

```

--------------------------------

### Databricks Complete Metric View Definition Example

Source: https://docs.databricks.com/aws/en/metric-views/semantic-metadata

This comprehensive YAML example defines a metric view in Databricks, incorporating various semantic metadata types including date formatting for 'order_date', currency formatting for 'total_revenue' and 'avg_order_value', and number formatting for 'order_count'. It showcases dimension and measure definitions with comments, display names, and synonyms.

```yaml
version: 1.1  
source: samples.tpch.orders  
comment: Comprehensive sales metrics with enhanced semantic metadata  
dimensions:  
  - name: order_date  
    expr: o_orderdate  
    comment: Date when the order was placed  
    display_name: Order Date  
    format:  
      type: date  
      date_format: year_month_day  
      leading_zeros: true  
    synonyms:  
      - order time  
      - date of order  
  - name: customer_segment  
    expr: |  
      CASE  
        WHEN o_totalprice > 100000 THEN 'Enterprise'  
        WHEN o_totalprice > 10000 THEN 'Mid-market'  
        ELSE 'SMB'  
      END  
    comment: Customer classification based on order value  
    display_name: Customer Segment  
    synonyms:  
      - segment  
      - customer tier  
measures:  
  - name: total_revenue  
    expr: SUM(o_totalprice)  
    comment: Total revenue from all orders  
    display_name: Total Revenue  
    format:  
      type: currency  
      currency_code: USD  
      decimal_places:  
        type: exact  
        places: 2  
      hide_group_separator: false  
      abbreviation: compact  
    synonyms:  
      - revenue  
      - total sales  
      - sales amount  
  - name: order_count  
    expr: COUNT(1)  
    comment: Total number of orders  
    display_name: Order Count  
    format:  
      type: number  
      decimal_places:  
        type: all  
      hide_group_separator: true  
    synonyms:  
      - count  
      - number of orders  
  - name: avg_order_value  
    expr: SUM(o_totalprice) / COUNT(1)  
    comment: Average revenue per order  
    display_name: Average Order Value  
    format:  
      type: currency  
      currency_code: USD  
      decimal_places:  
        type: exact  
        places: 2  
    synonyms:  
      - aov  
      - average revenue  

```

--------------------------------

### SQL Query Using Metric View with MEASURE Aggregate

Source: https://docs.databricks.com/aws/en/dashboards/datasets

This SQL query shows how to reference a metric view and use the `MEASURE` aggregate function to access its defined measures. This approach allows for filtering and reshaping the dataset derived from the metric view.

```sql
SELECT
  dimension1,
  MEASURE(metric_name) AS aggregated_metric
FROM
  your_catalog.your_schema.your_metric_view
WHERE
  dimension1 = 'specific_dimension';
```

--------------------------------

### Create Databricks Metric View with YAML

Source: https://docs.databricks.com/gcp/ja/sql/language-manual/sql-ref-syntax-ddl-create-view

Illustrates the creation of a metric view in Databricks using a YAML definition. This type of view aggregates data and is defined with dimensions and measures, suitable for analytical purposes. The example shows how to define dimensions like 'month', 'status', and 'order_priority', and measures such as 'count_orders' and 'total_revenue'.

```sql
-- Creates a Metric View as specified in the YAML definition, with three dimensions and four measures representing the count of orders.  
> CREATE OR REPLACE VIEW region_sales_metrics  
  (month COMMENT 'Month order was made',  
   status,  
   order_priority,  
   count_orders COMMENT 'Count of orders',  
   total_Revenue,  
   total_revenue_per_customer,  
   total_revenue_for_open_orders)  
  WITH METRICS  
  LANGUAGE YAML  
  COMMENT 'A Metric View for regional sales metrics.'  
  AS $$  
   version: 0.1  
   source: samples.tpch.orders  
   filter: o_orderdate > '1990-01-01'  
   dimensions:  
   - name: month  
     expr: date_trunc('MONTH', o_orderdate)  
   - name: status  
     expr: case  
       when o_orderstatus = 'O' then 'Open'  
       when o_orderstatus = 'P' then 'Processing'  
       when o_orderstatus = 'F' then 'Fulfilled'  
       end  
   - name: prder_priority  
     expr: split(o_orderpriority, '-')[1]  
   measures:  
   - name: count_orders  
     expr: count(1)  
   - name: total_revenue  
     expr: SUM(o_totalprice)  
   - name: total_revenue_per_customer  
     expr: SUM(o_totalprice) / count(distinct o_custkey)  
   - name: total_revenue_for_open_orders  
     expr: SUM(o_totalprice) filter (where o_orderstatus='O')  
  $$;  
  
> DESCRIBE EXTENDED region_sales_metrics;  
  col_name                       data_type  
  ------------------------------ --------------------------  
  month                          timestamp  
  status                         string  
  order_priority                 string  
  count_orders                   bigint measure  
  total_revenue                  decimal(28,2) measure  
  total_revenue_per_customer     decimal(38,12) measure  
  total_revenue_for_open_orders  decimal(28,2) measure  
  
  # Detailed Table Information  
  Catalog                        main  
  Database                       default  
  Table                          region_sales_metrics  
  Owner                          alf@melmak.et  
  Created Time                   Thu May 15 13:03:01 UTC 2025  
  Last Access                    UNKNOWN  
  Created By                     Spark  
  Type                           METRIC_VIEW  
  Comment                        A Metric View for regional sales metrics.  
  Use Remote Filtering           false  
```

--------------------------------

### Define Dimensions for a Metric View

Source: https://docs.databricks.com/aws/en/metric-views/yaml-ref

This YAML snippet illustrates the definition of dimensions within a Databricks metric view. It covers basic column referencing, SQL expression-based dimensions, handling column names with spaces, and multi-line CASE statements for conditional dimension values.

```yaml
dimensions:
  # Column name
  - name: Order date
    expr: o_orderdate

  # SQL expression
  - name: Order month
    expr: DATE_TRUNC('MONTH', `Order date`)

  # Referring to a column with a space in the name
  - name: Month of order
    expr: `Order month`

  # Multi-line expression
  - name: Order status
    expr: CASE
            WHEN o_orderstatus = 'O' THEN 'Open'
            WHEN o_orderstatus = 'P' THEN 'Processing'
            WHEN o_orderstatus = 'F' THEN 'Fulfilled'
          END

```

--------------------------------

### SQL: TEMPORARY Keyword for Metric View Creation

Source: https://docs.databricks.com/gcp/pt/release-notes/serverless

Support for the TEMPORARY keyword when creating metric views in Databricks SQL. Temporary metric views are session-scoped and automatically dropped when the session ends, useful for ad-hoc analysis.

```sql
CREATE TEMPORARY VIEW my_temp_view AS SELECT * FROM my_table
```

--------------------------------

### Describe Extended Metric View Metadata in JSON

Source: https://docs.databricks.com/gcp/ja/sql/language-manual/sql-ref-syntax-aux-describe-table

This command retrieves the detailed metadata of a metric view in JSON format. It outlines the view's name, catalog, schema, columns with their types and measure properties, owner, creation time, and the underlying view text definition. This is valuable for understanding the structure and logic of metric views.

```sql
-- The JSON describe of a metric view
> DESCRIBE EXTENDED region_sales_metrics AS JSON;
{
  "table_name":"region_sales_metrics",
  "catalog_name":"main",
  "namespace":["default"],
  "schema_name":"default",
  "columns":[
    {"name":"month","type":{"name":"timestamp_ltz"},"nullable":true},
    {"name":"status","type":{"name":"string","collation":"UTF8_BINARY"},"nullable":true},
    {"name":"prder_priority","type":{"name":"string","collation":"UTF8_BINARY"},"nullable":true},
    {"name":"count_orders","type":{"name":"bigint"},"nullable":false,"is_measure":true},
    {"name":"total_revenue","type":{"name":"decimal","precision":28,"scale":2},"nullable":true,"is_measure":true},
    {"name":"total_revenue_per_customer","type":{"name":"decimal","precision":38,"scale":12},"nullable":true,"is_measure":true}],
  "owner":"alf@melmak.et",
  "created_time":"2025-05-18T23:45:25Z",
  "last_access":"UNKNOWN",
  "created_by":"Spark ",
  "type":"METRIC_VIEW",
  "comment":"A metric view for regional sales metrics.",
  "view_text":"\n version: 0.1\n source: samples.tpch.orders\n filter: o_orderdate > '1990-01-01'\n dimensions:\n - name: month\n expr: date_trunc('MONTH', o_orderdate)\n - name: status\n expr: case\n when o_orderstatus = 'O' then 'Open'\n when o_orderstatus = 'P' then 'Processing'\n when o_orderstatus = 'F' then 'Fulfilled'\n end\n - name: prder_priority\n expr: split(o_orderpriority, '-')[1]\n measures:\n - name: count_orders\n expr: count(1)\n - name: total_revenue\n expr: SUM(o_totalprice)\n - name: total_revenue_per_customer\n expr: SUM(o_totalprice) / count(distinct o_custkey)\n ","language":"YAML","table_properties":{"metric_view.from.name":"samples.tpch.orders","metric_view.from.type":"ASSET","metric_view.where":"o_orderdate > '1990-01-01'"},
  "view_creation_spark_configuration":{ ... },
  "collation":"UTF8_BINARY"}
```

--------------------------------

### Get Metric View Metadata (JSON)

Source: https://docs.databricks.com/aws/ja/sql/language-manual/sql-ref-syntax-aux-describe-table

Retrieves the metadata for a specified metric view in JSON format. This includes details about dimensions, measures, and view definition.

```APIDOC
## GET /api/metric-views/{viewName}/metadata

### Description
Returns the metadata for a specified metric view in JSON format.

### Method
GET

### Endpoint
`/api/metric-views/{viewName}/metadata`

### Parameters
#### Path Parameters
- **viewName** (string) - Required - The name of the metric view to retrieve metadata for.

### Request Example
```json
{
  "query": "DESCRIBE EXTENDED region_sales_metrics AS JSON;"
}
```

### Response
#### Success Response (200)
- **table_name** (string) - The name of the metric view.
- **catalog_name** (string) - The catalog the metric view belongs to.
- **schema_name** (string) - The schema the metric view belongs to.
- **columns** (array) - An array of column objects, including measures and dimensions.
- **owner** (string) - The owner of the metric view.
- **created_time** (string) - The timestamp when the metric view was created.
- **type** (string) - The type of the asset, 'METRIC_VIEW'.
- **comment** (string) - A description of the metric view.
- **view_text** (string) - The definition of the metric view.

#### Response Example
```json
{
  "table_name":"region_sales_metrics",
  "catalog_name":"main",
  "namespace":["default"],
  "schema_name":"default",
  "columns":[
    {"name":"month","type":{"name":"timestamp_ltz"},"nullable":true},
    {"name":"status","type":{"name":"string","collation":"UTF8_BINARY"},"nullable":true},
    {"name":"prder_priority","type":{"name":"string","collation":"UTF8_BINARY"},"nullable":true},
    {"name":"count_orders","type":{"name":"bigint"},"nullable":false,"is_measure":true},
    {"name":"total_revenue","type":{"name":"decimal","precision":28,"scale":2},"nullable":true,"is_measure":true},
    {"name":"total_revenue_per_customer","type":{"name":"decimal","precision":38,"scale":12},"nullable":true,"is_measure":true}],
  "owner":"alf@melmak.et",
  "created_time":"2025-05-18T23:45:25Z",
  "last_access":"UNKNOWN",
  "created_by":"Spark ",
  "type":"METRIC_VIEW",
  "comment":"A metric view for regional sales metrics.",
  "view_text":"\n version: 0.1\n source: samples.tpch.orders\n filter: o_orderdate > '1990-01-01'\n dimensions:\n - name: month\n expr: date_trunc('MONTH', o_orderdate)\n - name: status\n expr: case\n when o_orderstatus = 'O' then 'Open'\n when o_orderstatus = 'P' then 'Processing'\n when o_orderstatus = 'F' then 'Fulfilled'\n end\n - name: prder_priority\n expr: split(o_orderpriority, '-')[1]\n measures:\n - name: count_orders\n expr: count(1)\n - name: total_revenue\n expr: SUM(o_totalprice)\n - name: total_revenue_per_customer\n expr: SUM(o_totalprice) / count(distinct o_custkey)\n ","language":"YAML","table_properties":{"metric_view.from.name":"samples.tpch.orders","metric_view.from.type":"ASSET","metric_view.where":"o_orderdate > '1990-01-01'"},
  "view_creation_spark_configuration":{ ... },
  "collation":"UTF8_BINARY"}
```
```

--------------------------------

### Databricks YAML: Composing Dimensions and Measures

Source: https://docs.databricks.com/gcp/en/metric-views/yaml-ref

Demonstrates how to define dimensions and measures in a Databricks metric view YAML file, showcasing how new elements can reference previously defined ones. This supports building complex logic through composition.

```yaml
dimensions:  
  
  # Dimension referencing a source column  
  - name: Order month  
    expr: DATE_TRUNC('month', o_orderdate)  
  
  # Dimension referencing a previously defined dimension  
  - name: Previous order month  
    expr: ADD_MONTHS(`Order Month`, -1)  
  
measures:  
  
  # Measure referencing a dimension  
  - name: Earliest order month  
    expr: MIN(`Order month`)  
  
  # Measure referencing a source column  
  - name: Revenue  
    expr: SUM(sales_amount)  
  
  # Measure referencing a source column  
  - name: Costs  
    expr: SUM(item_cost)  
  
  # Measure referencing previously defined measures  
  - name: Profit  
    expr: MEASURE(Revenue) - MEASURE(Costs)  
  

```

--------------------------------

### SQL: Create Temporary Metric Views

Source: https://docs.databricks.com/aws/en/release-notes/serverless

Allows the use of the TEMPORARY keyword when creating metric views. Temporary metric views are session-scoped and automatically dropped when the session ends, providing transient data exploration capabilities.

```sql
CREATE TEMPORARY VIEW temp_metric_view AS SELECT metric_name, metric_value FROM source_table WHERE date = CURRENT_DATE();
```

--------------------------------

### SQL - Creating Temporary Metric Views

Source: https://docs.databricks.com/gcp/pt/release-notes/runtime/17

Demonstrates the use of the `TEMPORARY` keyword when creating a metric view. Temporary views are session-specific and are automatically dropped when the session ends, aiding in temporary data analysis or manipulation.

```sql
CREATE TEMPORARY VIEW my_temp_view AS
SELECT column1, column2
FROM my_table
WHERE condition;
```

--------------------------------

### Configure currency format in Databricks metric view YAML

Source: https://docs.databricks.com/gcp/en/metric-views/semantic-metadata

Illustrates how to set currency formatting for a Databricks metric view using YAML. This includes specifying the currency code (USD), exact decimal places, and compact abbreviation.

```yaml
format:
  type: currency
  currency_code: USD
  decimal_places:
    type: exact
    places: 2
  hide_group_separator: false
  abbreviation: compact

```

--------------------------------

### Define Metrics View Source as SQL Query - YAML

Source: https://docs.databricks.com/aws/pt/metric-views/yaml-ref

This YAML snippet shows how to use a SQL query directly as the data source for a metrics view. It supports complex queries including JOIN clauses.

```yaml
source: SELECT * FROM samples.tpch.orders o  
  LEFT JOIN samples.tpch.customer c  
  ON o.o_custkey = c.c_custkey  
  

```

--------------------------------

### Databricks SQL: Use TEMPORARY keyword for metric view creation

Source: https://docs.databricks.com/gcp/pt/sql/release-notes/2025

Shows the usage of the TEMPORARY keyword when creating a metric view in Databricks SQL. Temporary metric views are session-specific and automatically dropped at the end of the session, useful for ad-hoc analysis.

```sql
CREATE TEMPORARY VIEW my_temp_metric_view AS SELECT ...;
```

--------------------------------

### Query Metric View in Databricks SQL

Source: https://docs.databricks.com/gcp/en/sql/user/sql-editor/legacy

This SQL snippet demonstrates how to query a metric view, aggregating measures over specified dimensions and sorting the results. It requires all measure evaluations to be wrapped in the MEASURE function. The source table is assumed to be from the 'samples' catalog.

```sql
SELECT  
 `Order Month`,  
 `Order Status`,  
 MEASURE(`Order Count`),  
 MEASURE(`Total Revenue`),  
 MEASURE(`Total Revenue per Customer`)  
FROM  
 orders_metric_view  
GROUP BY ALL  
ORDER BY 1 ASC;
```

--------------------------------

### Databricks SQL: Using TEMPORARY Keyword for Metric View Creation

Source: https://docs.databricks.com/aws/en/sql/release-notes/2025

This SQL snippet demonstrates the usage of the `TEMPORARY` keyword when creating a metric view in Databricks SQL. Temporary metric views are session-scoped and automatically dropped at the end of the session.

```sql
CREATE TEMPORARY VIEW my_temp_view AS SELECT * FROM my_table;

```

--------------------------------

### Query Metric View in Databricks SQL

Source: https://docs.databricks.com/aws/en/sql/user/sql-editor

This SQL code illustrates how to query a metric view. It uses the MEASURE function to evaluate specific metrics and aggregates the results by 'Order Month' and 'Order Status', sorting the output by 'Order Month'. Identifiers with spaces are enclosed in backticks. Ensure the 'orders_metric_view' exists and the specified measures are defined.

```sql

SELECT  
 `Order Month`,  
 `Order Status`,  
 MEASURE(`Order Count`),  
 MEASURE(`Total Revenue`),  
 MEASURE(`Total Revenue per Customer`)  
FROM  
 orders_metric_view  
GROUP BY ALL  
ORDER BY 1 ASC;  

```

--------------------------------

### Describe Extended Metric View Metadata (SQL)

Source: https://docs.databricks.com/gcp/pt/sql/language-manual/sql-ref-syntax-aux-describe-table

Retrieves detailed metadata for a metric view in JSON format. This includes schema information, owner, creation details, and the underlying 'view_text' which defines the metric view's logic, dimensions, and measures.

```sql
DESCRIBE EXTENDED region_sales_metrics AS JSON;
```