## Correct Join Syntax for Databricks Metric Views

There are **two distinct types of joins** when working with Databricks metric views, and it's critical to understand the difference:

### 1. Joins Defined **Within** a Metric View (YAML Definition)

These joins are defined in the YAML specification when **creating** the metric view and are used to build star schema or snowflake schema relationships[1][2]. All joins defined within metric views use **LEFT OUTER JOIN** semantics[1][2].

#### Star Schema Join Syntax

For star schema designs where you join a fact table directly to dimension tables[1][2]:

**Using ON Clause (Boolean Expression):**

```yaml
version: 1.1
source: catalog.schema.fact_table

joins:
  # ON clause with boolean expression
  - name: dimension_table_1
    source: catalog.schema.dimension_table_1
    'on': source.dimension_table_1_fk = dimension_table_1.pk
  
  - name: customers
    source: catalog.schema.customers
    'on': source.customer_id = customers.customer_id

dimensions:
  # Reference joined table columns using dot notation
  - name: Customer Name
    expr: customers.customer_name
  
  - name: Dimension Key
    expr: dimension_table_1.pk

measures:
  - name: Total Revenue
    expr: SUM(source.revenue)
```

**Important Note:** The keyword `on` should be wrapped in quotes (`'on'`) because YAML 1.1 parsers (like PyYAML) can misinterpret unquoted keys such as `on`, `off`, `yes`, `no`, or `NO` as boolean values, which causes join errors[1][2].

**Using USING Clause (Column Array):**

```yaml
source: catalog.schema.orders

joins:
  # USING clause for columns with same name in both tables
  - name: products
    source: catalog.schema.products
    using:
      - product_id
      - store_id

dimensions:
  - name: Product Name
    expr: products.product_name
```

The `USING` clause lists columns that have **the same name** in both the parent table (source) and the joined table[1][2].

#### Snowflake Schema Join Syntax (Nested Joins)

For snowflake schemas where you have multi-hop joins through dimension tables to subdimension tables[1][2]:

**Requires Databricks Runtime 17.1 or above**[3][1][2].

```yaml
version: 1.1
source: samples.tpch.orders

joins:
  # First-level join to dimension
  - name: customer
    source: samples.tpch.customer
    'on': source.o_custkey = customer.c_custkey
    
    # Nested join from customer to nation (subdimension)
    joins:
      - name: nation
        source: samples.tpch.nation
        'on': customer.c_nationkey = nation.n_nationkey
        
        # Further nested join from nation to region
        joins:
          - name: region
            source: samples.tpch.region
            'on': nation.n_regionkey = region.r_regionkey

dimensions:
  - name: Clerk
    expr: o_clerk
  
  # Access nested dimensions using dot notation
  - name: Customer Name
    expr: customer.c_name
  
  - name: Nation Name
    expr: customer.nation.n_name
  
  - name: Region Name
    expr: customer.nation.region.r_name
  
  # Return full struct from joined table
  - name: Customer Object
    expr: customer  # returns full customer row as struct

measures:
  - name: Total Orders
    expr: COUNT(*)
  
  - name: Total Revenue
    expr: SUM(o_totalprice)
```

**Key Points:**

- The `source` namespace refers to the metric view's source (fact table)[1][2]
- Join names (like `customer`, `nation`) refer to the joined tables[1][2]
- Nested joins are indented under the parent join's `joins` key[1][2]
- Use dot notation to access columns from nested joins (e.g., `customer.nation.n_name`)[1][2]
- Joins should follow **many-to-one** relationships; in many-to-many cases, the first matching row is selected[1][2]

#### Joining to Other Metric Views

You can join a metric view to **another metric view**[3][4][5]:

```yaml
source: catalog.schema.fact_sales

joins:
  - name: customer_metrics
    source: catalog.schema.customer_metric_view  # Another metric view
    'on': source.customer_id = customer_metrics.customer_id

dimensions:
  # Only dimensions from joined metric view are available
  - name: Customer Segment
    expr: customer_metrics.segment
```

**Important Limitation:** When joining to another metric view, **only its dimensions are available** in the downstream metric view—you cannot access its measures[3][4].

### 2. Query-Time Joins Between Metric Views

**Critical Limitation: Query-time joins of metric views are NOT supported**[6][7].

You **cannot** write SQL queries that join multiple metric views together like this:

```sql
-- ❌ THIS DOES NOT WORK
SELECT 
  mv1.dimension1,
  MEASURE(mv1.measure1),
  MEASURE(mv2.measure2)
FROM metric_view_1 mv1
JOIN metric_view_2 mv2
  ON mv1.customer_id = mv2.customer_id
GROUP BY mv1.dimension1;
```

This is one of the **key limitations** of metric views[6][7]. According to Databricks documentation and demonstrations:

- **`SELECT *` is not supported** on metric views[6][7]
- **Joins at query time are not supported**[6][7]
- If you need to combine data from multiple tables or views, you must define the joins **within the metric view YAML definition** or join the underlying tables **before** creating the metric view[6][7]

### Correct Query Syntax for Metric Views

When querying a metric view, use this syntax[8][9][10]:

```sql
-- ✅ CORRECT: Query a single metric view
SELECT 
  `Order Month`,
  `Order Status`,
  MEASURE(`Order Count`),
  MEASURE(`Total Revenue`),
  MEASURE(`Average Order Value`)
FROM catalog.schema.orders_metric_view
WHERE `Order Status` = 'Fulfilled'
GROUP BY ALL
ORDER BY `Order Month` ASC;
```

**Key Requirements:**

- All **measures must be wrapped** in the `MEASURE()` aggregate function[8][9][10]
- Dimensions can be used in `SELECT`, `WHERE`, and `GROUP BY` clauses[3][4]
- Use backticks (`` ` ``) for column names with spaces[9][11]
- You must be connected to a **SQL warehouse or compute running Databricks Runtime 16.4 or above**[8][9][11]

### Composability and Metric View Chaining

While you can't join metric views at query time, you **can build metric views on top of other metric views** for composability[5][12]:

```yaml
# Regional metric view built on top of global metric view
version: 1.1
source: catalog.schema.global_company_metric_view

filter: region = 'APAC'  # Add regional filter

dimensions:
  # Inherit dimensions from source metric view
  - name: Month
    expr: month_dimension
  
  - name: Product Category
    expr: product_category

measures:
  # Define new measures or reference existing ones
  - name: Regional Revenue
    expr: SUM(revenue)
  
  - name: Regional Order Count
    expr: COUNT(order_id)
```

This approach allows you to create layered, reusable logic without duplicating calculations[13][5][12].

### Common YAML Syntax Errors to Avoid

**1. Unquoted reserved keywords:**

```yaml
# ❌ WRONG - 'on' interpreted as boolean
joins:
  - name: customers
    source: catalog.schema.customers
    on: source.customer_id = customers.id

# ✅ CORRECT - quote the keyword
joins:
  - name: customers
    source: catalog.schema.customers
    'on': source.customer_id = customers.id
```

**2. Incorrect indentation:**

```yaml
# ❌ WRONG - joins not properly nested
joins:
- name: customer
  source: samples.tpch.customer
  'on': source.o_custkey = customer.c_custkey
joins:  # This should be indented under the customer join
  - name: nation
    source: samples.tpch.nation

# ✅ CORRECT - nested joins properly indented
joins:
  - name: customer
    source: samples.tpch.customer
    'on': source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        'on': customer.c_nationkey = nation.n_nationkey
```

**3. Mixing ON and USING clauses:**

Use one or the other consistently within each join definition[1][2].

### Summary

**For joins within metric view definitions:**
- Use `'on'` clause with boolean expressions or `using` clause with column arrays[1][2]
- All joins are LEFT OUTER JOIN by default[1][2]
- Star schema: first-level joins to dimensions[1][2]
- Snowflake schema: nested joins require DBR 17.1+[3][1][2]
- Can join to other metric views (dimensions only)[3][4]

**For querying metric views:**
- Query-time joins between metric views are **not supported**[6][7]
- Use `MEASURE()` function for all measures[8][9][10]
- Connect to DBR 16.4+ warehouse or cluster[8][9][11]
- Define all needed joins in the YAML before querying[6][7]


