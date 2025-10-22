# ACME Retail Simulator - Database Schema

This document provides a comprehensive overview of all database tables in the ACME Retail Simulator.

## Reference Data

### brand
- brand_id: Integer (PK, autoincrement)
- name: String(255)
- status: String(50) [default: 'active'] | Actual values: ['active']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### category
- category_id: Integer (PK, autoincrement)
- name: String(255)
- parent_category_id: Integer (FK: category.category_id, nullable)
- level: Integer
- path: String(500, nullable)
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### unit_of_measure
- uom_id: Integer (PK, autoincrement)
- code: String(20, unique)
- name: String(100)
- uom_type: String(50) [weight, volume, count]
- last_update_dttm: DateTime

### price_list
- price_list_id: Integer (PK, autoincrement)
- name: String(100)
- type: String(50, nullable)
- effective_from: DateTime (nullable)
- effective_to: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: []
- last_update_dttm: DateTime

### reason_code
- reason_code_id: Integer (PK, autoincrement)
- category: String(50) [adjustment, return, damage]
- code: String(50)
- description: String(255)
- last_update_dttm: DateTime

### payment_method
- method_id: Integer (PK, autoincrement)
- code: String(50) [CASH, CREDIT, DEBIT, MOBILE]
- name: String(100)
- is_active: Boolean [default: True]
- last_update_dttm: DateTime

### carrier
- carrier_id: Integer (PK, autoincrement)
- name: String(255)
- contact_info: String(500, nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- last_update_dttm: DateTime

### route
- route_id: Integer (PK, autoincrement)
- name: String(255)
- carrier_id: Integer (FK: carrier.carrier_id, nullable)
- status: String(50) [default: 'active'] | Actual values: []
- last_update_dttm: DateTime

### user
- user_id: Integer (PK, autoincrement)
- username: String(100, unique)
- first_name: String(100)
- last_name: String(100)
- email: String(255)
- role: String(50) [cashier, picker, manager]
- store_id: Integer (nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### simulation_run
- run_id: Integer (PK, autoincrement)
- business_date: DateTime [simulation/business date]
- run_type: String(50) [bootstrap, daily]
- started_at: DateTime [wall clock start time]
- completed_at: DateTime (nullable) [wall clock end time]
- status: String(50) [running, completed, failed] | Actual values: ['completed']
- error_message: String(500, nullable)
- last_update_dttm: DateTime

### tax_rate
- tax_rate_id: Integer (PK, autoincrement)
- name: String(100)
- jurisdiction: String(100) [US, CA-ON, etc]
- rate: Numeric(5,4) [0.1000 for 10%]
- effective_from: DateTime
- effective_to: DateTime (nullable)
- last_update_dttm: DateTime

### payment_terms
- payment_terms_id: Integer (PK, autoincrement)
- name: String(50) [Net 30, Net 60, etc]
- description: String(255, nullable)
- days: Integer [number of days]
- last_update_dttm: DateTime

---

## ERP (Enterprise Resource Planning)

### location
- location_id: Integer (PK, autoincrement)
- location_type: String(50) [store, warehouse]
- name: String(255)
- address_line1: String(255, nullable)
- address_line2: String(255, nullable)
- city: String(100, nullable)
- state: String(50, nullable)
- zip_code: String(20, nullable)
- country: String(100, nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- last_update_dttm: DateTime

### product
- product_id: Integer (PK, autoincrement)
- sku: String(100, unique)
- upc: String(20, nullable)
- name: String(500)
- brand_id: Integer (FK: brand.brand_id, nullable)
- category_id: Integer (FK: category.category_id, nullable)
- uom_id: Integer (FK: unit_of_measure.uom_id, nullable)
- open_food_facts_code: String(100, nullable)
- quantity: String(100, nullable)
- packaging: String(255, nullable)
- nutrition_data: JSON (nullable)
- base_cost: Numeric(10,2, nullable)
- base_price: Numeric(10,2, nullable)
- reorder_quantity: Integer (nullable)
- shelf_life_days: Integer (nullable)
- status: String(50) [default: 'active'] | Actual values: ['active', 'discontinued']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### supplier
- vendor_id: Integer (PK, autoincrement)
- name: String(255)
- tax_id: String(50, nullable)
- payment_terms_id: Integer (FK: payment_terms.payment_terms_id, nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### supplier_brand
- supplier_brand_id: Integer (PK, autoincrement)
- vendor_id: Integer (FK: supplier.vendor_id)
- brand_id: Integer (FK: brand.brand_id)
- is_primary: Boolean [default: True]
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### supplier_item
- supplier_item_id: Integer (PK, autoincrement)
- vendor_id: Integer (FK: supplier.vendor_id)
- product_id: Integer (FK: product.product_id)
- vendor_sku: String(100)
- lead_time_days: Integer
- min_order_qty: Integer [default: 1]
- pack_size: Integer [default: 1]
- last_cost: Numeric(10,2, nullable)
- currency: String(3) [default: 'USD']
- last_update_dttm: DateTime

### supplier_product_availability
- availability_id: Integer (PK, autoincrement)
- vendor_id: Integer (FK: supplier.vendor_id)
- product_id: Integer (FK: product.product_id)
- available_qty: Integer [default: 0]
- reserved_qty: Integer [default: 0]
- stockout_until: Date (nullable)
- lead_time_days: Integer [default: 3]
- last_restocked_at: DateTime (nullable)
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### purchase_order_backorder
- backorder_id: Integer (PK, autoincrement)
- po_line_id: Integer (FK: purchase_order_line.po_line_id)
- product_id: Integer (FK: product.product_id)
- vendor_id: Integer (FK: supplier.vendor_id)
- ordered_qty: Integer
- fulfilled_qty: Integer [default: 0]
- unfulfilled_qty: Integer
- requested_date: Date
- expected_fulfillment_date: Date (nullable)
- fulfilled_date: Date (nullable)
- status: String(50) [default: 'pending'] [pending, partial, fulfilled, cancelled] | Actual values: []
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### lost_sales_event
- event_id: Integer (PK, autoincrement)
- event_date: DateTime
- event_type: String(50) [pos, ecommerce, transfer]
- location_id: Integer (FK: location.location_id, nullable)
- product_id: Integer (FK: product.product_id)
- lost_qty: Integer
- estimated_value: Numeric(10,2, nullable)
- source_ref: String(100, nullable)
- last_update_dttm: DateTime

### product_price
- product_price_id: Integer (PK, autoincrement)
- product_id: Integer (FK: product.product_id)
- price_list_id: Integer (FK: price_list.price_list_id)
- currency: String(3) [default: 'USD']
- price: Numeric(10,2)
- effective_from: DateTime
- effective_to: DateTime (nullable)
- last_update_dttm: DateTime

### purchase_order
- po_id: Integer (PK, autoincrement)
- vendor_id: Integer (FK: supplier.vendor_id)
- order_date: Date
- expected_date: Date (nullable)
- currency: String(3) [default: 'USD']
- ship_to_location_id: Integer (FK: location.location_id)
- status: String(50) [default: 'active'] | Actual values: ['received']
- last_update_dttm: DateTime

### purchase_order_line
- po_line_id: Integer (PK, autoincrement)
- po_id: Integer (FK: purchase_order.po_id)
- product_id: Integer (FK: product.product_id)
- ordered_qty: Integer
- uom_id: Integer (FK: unit_of_measure.uom_id)
- unit_price: Numeric(10,2)
- tax_rate: Numeric(5,2)
- due_date: Date (nullable)
- status: String(50) [default: 'active'] | Actual values: ['received']
- last_update_dttm: DateTime

### goods_receipt
- grn_id: Integer (PK, autoincrement)
- po_id: Integer (FK: purchase_order.po_id)
- receipt_date: Date
- received_by_user_id: Integer (FK: user.user_id)
- status: String(50) [default: 'active'] | Actual values: ['completed']
- last_update_dttm: DateTime

### goods_receipt_line
- grn_line_id: Integer (PK, autoincrement)
- grn_id: Integer (FK: goods_receipt.grn_id)
- po_line_id: Integer (FK: purchase_order_line.po_line_id)
- product_id: Integer (FK: product.product_id)
- received_qty: Integer
- rejected_qty: Integer [default: 0]
- reason_code_id: Integer (FK: reason_code.reason_code_id, nullable)
- last_update_dttm: DateTime

### transfer_order
- transfer_id: Integer (PK, autoincrement)
- source_location_id: Integer (FK: location.location_id)
- dest_location_id: Integer (FK: location.location_id)
- created_at: DateTime
- shipped_at: DateTime (nullable)
- received_at: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: ['delivered', 'pending', 'shipped']
- last_update_dttm: DateTime

### transfer_order_line
- transfer_line_id: Integer (PK, autoincrement)
- transfer_id: Integer (FK: transfer_order.transfer_id)
- product_id: Integer (FK: product.product_id)
- qty: Integer
- uom_id: Integer (FK: unit_of_measure.uom_id)
- last_update_dttm: DateTime

---

## CRM (Customer Relationship Management)

### customer
- customer_id: Integer (PK, autoincrement)
- external_ref: String(100, nullable)
- first_name: String(100)
- last_name: String(100)
- email: String(255)
- phone: String(50, nullable)
- dob: Date (nullable)
- segment: String(50, nullable) [budget, premium, family, convenience]
- status: String(50) [default: 'active'] | Actual values: ['active']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### customer_address
- address_id: Integer (PK, autoincrement)
- customer_id: Integer (FK: customer.customer_id)
- line1: String(255)
- line2: String(255, nullable)
- city: String(100)
- state: String(50)
- postcode: String(20)
- country: String(100)
- is_default_shipping: Boolean [default: False]
- is_default_billing: Boolean [default: False]
- last_update_dttm: DateTime

### customer_payment_method
- payment_method_ref_id: Integer (PK, autoincrement)
- customer_id: Integer (FK: customer.customer_id)
- payment_method_id: Integer (FK: payment_method.method_id)
- nickname: String(100, nullable)
- is_default: Boolean [default: False]
- payment_token: String(255, unique) [tokenized reference]
- card_details: JSON (nullable) [masked display data]
- billing_address_id: Integer (FK: customer_address.address_id, nullable)
- expires_at: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

---

## Inventory Management

### store_inventory
- store_inventory_id: Integer (PK, autoincrement)
- store_id: Integer (FK: location.location_id)
- product_id: Integer (FK: product.product_id)
- on_hand_qty: Integer [default: 0]
- reserved_qty: Integer [default: 0]
- safety_stock_qty: Integer (nullable)
- reorder_point: Integer (nullable)
- last_update_dttm: DateTime

### warehouse_inventory
- wh_inventory_id: Integer (PK, autoincrement)
- warehouse_id: Integer (FK: warehouse.warehouse_id)
- product_id: Integer (FK: product.product_id)
- on_hand_qty: Integer [default: 0]
- reserved_qty: Integer [default: 0]
- last_update_dttm: DateTime

### store_inventory_transaction
- store_inv_txn_id: Integer (PK, autoincrement)
- store_id: Integer (FK: location.location_id)
- product_id: Integer (FK: product.product_id)
- txn_type: String(50) [receipt, sale, adjustment, transfer_in, transfer_out, return, waste]
- qty: Integer
- source_ref: String(255, nullable)
- user_id: Integer (FK: user.user_id, nullable)
- last_update_dttm: DateTime

### store_stock_adjustment
- adjustment_id: Integer (PK, autoincrement)
- store_id: Integer (FK: location.location_id)
- reason_code_id: Integer (FK: reason_code.reason_code_id)
- notes: String(500, nullable)
- user_id: Integer (FK: user.user_id)
- last_update_dttm: DateTime

### store_stock_adjustment_line
- adjustment_line_id: Integer (PK, autoincrement)
- adjustment_id: Integer (FK: store_stock_adjustment.adjustment_id)
- product_id: Integer (FK: product.product_id)
- qty_delta: Integer
- last_update_dttm: DateTime

### warehouse_inventory_transaction
- wh_inv_txn_id: Integer (PK, autoincrement)
- warehouse_id: Integer (FK: warehouse.warehouse_id)
- product_id: Integer (FK: product.product_id)
- txn_type: String(50) [receipt, shipment, adjustment, transfer_in, transfer_out, return]
- qty: Integer
- source_ref: String(255, nullable)
- user_id: Integer (FK: user.user_id, nullable)
- last_update_dttm: DateTime

---

## Warehouse Operations

### warehouse
- warehouse_id: Integer (PK, autoincrement)
- name: String(255)
- location_id: Integer (FK: location.location_id)
- status: String(50) [default: 'active'] | Actual values: ['active']
- last_update_dttm: DateTime

### warehouse_receipt
- receipt_id: Integer (PK, autoincrement)
- warehouse_id: Integer (FK: warehouse.warehouse_id)
- grn_id: Integer (FK: goods_receipt.grn_id, nullable)
- received_at: DateTime
- status: String(50) [default: 'active'] | Actual values: ['completed']
- last_update_dttm: DateTime

### warehouse_receipt_line
- receipt_line_id: Integer (PK, autoincrement)
- receipt_id: Integer (FK: warehouse_receipt.receipt_id)
- product_id: Integer (FK: product.product_id)
- qty: Integer
- uom_id: Integer (FK: unit_of_measure.uom_id)
- last_update_dttm: DateTime

### warehouse_shipment
- shipment_id: Integer (PK, autoincrement)
- warehouse_id: Integer (FK: warehouse.warehouse_id)
- transfer_id: Integer (FK: transfer_order.transfer_id, nullable)
- shipped_at: DateTime
- status: String(50) [default: 'active'] | Actual values: ['shipped']
- last_update_dttm: DateTime

### warehouse_shipment_line
- shipment_line_id: Integer (PK, autoincrement)
- shipment_id: Integer (FK: warehouse_shipment.shipment_id)
- product_id: Integer (FK: product.product_id)
- qty: Integer
- last_update_dttm: DateTime

---

## POS (Point of Sale)

### pos_transaction
- txn_id: Integer (PK, autoincrement)
- store_id: Integer (FK: location.location_id)
- terminal_id: String(50)
- cashier_user_id: Integer (FK: user.user_id)
- txn_datetime: DateTime
- customer_id: Integer (FK: customer.customer_id, nullable)
- status: String(50) | Actual values: ['completed']
- total_gross: Numeric(10,2)
- total_tax: Numeric(10,2)
- total_discount: Numeric(10,2)
- total_net: Numeric(10,2)
- payment_status: String(50)
- last_update_dttm: DateTime

### pos_transaction_line
- txn_line_id: Integer (PK, autoincrement)
- txn_id: Integer (FK: pos_transaction.txn_id)
- line_number: Integer
- product_id: Integer (FK: product.product_id)
- qty: Integer
- unit_price: Numeric(10,2)
- line_discount: Numeric(10,2) [default: 0]
- line_total: Numeric(10,2)
- last_update_dttm: DateTime

### pos_payment
- payment_id: Integer (PK, autoincrement)
- txn_id: Integer (FK: pos_transaction.txn_id)
- method_id: Integer (FK: payment_method.method_id)
- amount: Numeric(10,2)
- auth_code: String(100, nullable)
- captured_at: DateTime
- payment_details: JSON (nullable)
- last_update_dttm: DateTime

### pos_terminal
- terminal_id: Integer (PK, autoincrement)
- store_id: Integer (FK: location.location_id)
- name: String(100)
- terminal_number: String(50)
- installed_at: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: ['active']
- last_update_dttm: DateTime

### promotion
- promo_id: Integer (PK, autoincrement)
- name: String(255)
- type: String(50) [line_discount, basket_discount, bogo]
- rules_json: JSON (nullable)
- start_at: DateTime
- end_at: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: []
- last_update_dttm: DateTime

### promotion_application
- promo_app_id: Integer (PK, autoincrement)
- txn_id: Integer (FK: pos_transaction.txn_id)
- promo_id: Integer (FK: promotion.promo_id)
- discount_amount: Numeric(10,2)
- basis: String(20) [line or basket]
- last_update_dttm: DateTime

---

## E-commerce

### sales_order
- order_id: Integer (PK, autoincrement)
- order_number: String(50, unique)
- customer_id: Integer (FK: customer.customer_id)
- order_date: DateTime
- delivery_address_id: Integer (FK: customer_address.address_id)
- shipping_method: String(50)
- payment_method_id: Integer (FK: payment_method.method_id)
- payment_details: JSON (nullable)
- currency: String(3) [default: 'USD']
- subtotal: Numeric(10,2)
- tax_amount: Numeric(10,2)
- shipping_amount: Numeric(10,2)
- discount_amount: Numeric(10,2) [default: 0]
- total_amount: Numeric(10,2)
- fulfillment_status: String(50) [default: 'pending']
- warehouse_id: Integer (FK: location.location_id, nullable)
- estimated_delivery_date: Date (nullable)
- actual_delivery_date: Date (nullable)
- tracking_number: String(100, nullable)
- carrier_id: Integer (FK: carrier.carrier_id, nullable)
- status: String(50) [default: 'active'] | Actual values: ['allocated', 'completed', 'shipped']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### sales_order_line
- line_id: Integer (PK, autoincrement)
- order_id: Integer (FK: sales_order.order_id)
- line_number: Integer
- product_id: Integer (FK: product.product_id)
- qty: Integer
- unit_price: Numeric(10,2)
- discount_amount: Numeric(10,2) [default: 0]
- tax_amount: Numeric(10,2)
- line_total: Numeric(10,2)
- status: String(50) [default: 'pending'] | Actual values: ['allocated', 'shipped']
- allocated_qty: Integer [default: 0]
- shipped_qty: Integer [default: 0]
- cancelled_qty: Integer [default: 0]
- last_update_dttm: DateTime

### shipment_header
- shipment_id: Integer (PK, autoincrement)
- order_id: Integer (FK: sales_order.order_id)
- warehouse_id: Integer (FK: location.location_id)
- carrier_id: Integer (FK: carrier.carrier_id)
- tracking_number: String(100)
- ship_date: Date
- expected_delivery_date: Date (nullable)
- actual_delivery_date: Date (nullable)
- status: String(50) [default: 'active'] | Actual values: ['delivered', 'in_transit', 'out_for_delivery']
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### shipment_line
- shipment_line_id: Integer (PK, autoincrement)
- shipment_id: Integer (FK: shipment_header.shipment_id)
- order_line_id: Integer (FK: sales_order_line.line_id)
- product_id: Integer (FK: product.product_id)
- qty: Integer
- last_update_dttm: DateTime

---

## Loyalty Program

### loyalty_program
- program_id: Integer (PK, autoincrement)
- name: String(255)
- start_at: DateTime
- end_at: DateTime (nullable)
- status: String(50) [default: 'active'] | Actual values: []
- last_update_dttm: DateTime

### loyalty_account
- loyalty_account_id: Integer (PK, autoincrement)
- customer_id: Integer (FK: customer.customer_id)
- program_id: Integer (FK: loyalty_program.program_id)
- points_balance: Integer [default: 0]
- tier_id: Integer (nullable)
- status: String(50) [default: 'active'] | Actual values: []
- created_at: DateTime
- updated_at: DateTime
- last_update_dttm: DateTime

### loyalty_transaction
- loyalty_txn_id: Integer (PK, autoincrement)
- loyalty_account_id: Integer (FK: loyalty_account.loyalty_account_id)
- source_system: String(50) [POS, ECOM]
- source_ref_id: Integer
- txn_datetime: DateTime
- points_delta: Integer [positive for earn, negative for burn]
- reason: String(100) [earn, burn, adjustment, expiry]
- last_update_dttm: DateTime

---

## Financial & Accounting

### chart_of_accounts
- account_id: Integer (PK, autoincrement)
- account_code: String(50, unique)
- account_name: String(255)
- type: String(50) [asset, liability, equity, revenue, expense]
- parent_account_id: Integer (FK: chart_of_accounts.account_id, nullable)
- active: Boolean [default: True]
- last_update_dttm: DateTime

### journal_entry
- journal_id: Integer (PK, autoincrement)
- journal_date: DateTime
- description: String(500)
- source_system: String(50) [POS, ERP, ECOM, MANUAL]
- status: String(50) [draft, posted, reversed] | Actual values: []
- created_at: DateTime
- last_update_dttm: DateTime

### journal_entry_line
- journal_line_id: Integer (PK, autoincrement)
- journal_id: Integer (FK: journal_entry.journal_id)
- account_id: Integer (FK: chart_of_accounts.account_id)
- debit: Numeric(15,2) [default: 0]
- credit: Numeric(15,2) [default: 0]
- entity_type: String(50, nullable) [customer, supplier, product, location]
- entity_id: Integer (nullable)
- product_id: Integer (FK: product.product_id, nullable)
- last_update_dttm: DateTime