/* checks that all rows from stg would be inserted into dds and then will be deleted */
select 'dds.orders', source_id, max(order_id), min(order_id), count(*) from  dds.orders group by source_id
union all
select 'dds.products', source_id, max(product_id), min(product_id), count(*) from  dds.products group by source_id
union all
select 'dds.customers', source_id, max(customer_id), min(customer_id), count(*) from  dds.customers group by source_id
order by 2,1;