/* Initial model to bring customer and order data into single table.
    This will be decomposed into modular staging tables in next steps. */

/* Configure the model to be created as a persistent table */
{{ config ( materialized="table" ) }}

/* Customer CTE to reshape customer data */
with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers

),

/* Orders CTE to reshape order data */
orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from raw.jaffle_shop.orders

),

/* Aggregate order data according to business logic */
customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

/* Joined customer and order data for final data artifact */
final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final