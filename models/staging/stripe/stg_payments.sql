with payments as (
    
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as pymt_method,
        status,
        amount as pymt_amt,
        created as pymt_date

    from raw.stripe.payment
)

select * from payments