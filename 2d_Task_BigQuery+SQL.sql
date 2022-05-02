-- creating a separate column on product
create table solidtest.solid_product as
select a.id, product.id product_id, product.name, product.payment_action, product.currency, product.trial, product.amount, product.trial_period
from `holywater-test-task.solidtest.solid` a;

-- creating a separate column on invoices
create table solidtest.solid_invoices as
select a.id, i.id invoices_id, i.orders, i.updated_at	
,i.amount
,i.status
,i.created_at
,i.billing_period_started_at
,i.billing_period_ended_at
,i.subscription_term_number
from `holywater-test-task.solidtest.solid` a, UNNEST(invoices) as i;

-- creating a separate column on orders
create table solidtest.invoices_orders as
select a.id, a.invoices_id
,o.failed_reason
,o.id orders_id
,o.retry_attempt
,o.status
,o.amount
,o.created_at
,o.processed_at
,o.operation
from `holywater-test-task.solidtest.solid_invoices` a, unnest(orders) o;

-- 3
-- conversion by product
create table solidtest.solid_conversion as
select product_id, sum(not_1_invoice)/count(a.id) conversion
from (select a.id,
(case when count(invoices_id)>=2 then 1 else 0 end) not_1_invoice
from `holywater-test-task.solidtest.solid_invoices` a
where a.status='success'
group by a.id) a
join `holywater-test-task.solidtest.solid_product` b
on a.id = b.id
group by b.product_id
order by sum(not_1_invoice)/count(a.id) asc;

--function unavailable in the free tier acc:(
insert into solidtest.solid_conversion (product_id, conversion)
values(overall,  0.89904178176795579);

-- overall conversion
create table solidtest.solid_overall_conversion as
select sum(not_1_invoice)/count(id) conversion
from (
select a.id,
(case when count(invoices_id)>=2 then 1 else 0 end) not_1_invoice
from `holywater-test-task.solidtest.solid_invoices` a
where a.status = 'success'
group by a.id);

-- 4
-- LTV
create table solidtest.solid_overall_ltv as
select (avg(mrr) * avg(avg_sub_life))/avg(avg_active_subs) LTV
from
(select DATE_TRUNC(started_at, month) wave
,count(id)*avg(product.amount) MRR  -- monthly recurring revenue
,avg(date_diff(date(expired_at), date(started_at), month)) avg_sub_life -- avg subscription lifespan
,count(id) avg_active_subs -- avg active subscriptions per month
from `holywater-test-task.solidtest.solid`
group by DATE_TRUNC(started_at, month));

create table solidtest.solid_ltv as
select product_id
,(avg(mrr) * avg(avg_sub_life))/avg(avg_active_subs) LTV
from
(select DATE_TRUNC(started_at, month) wave
,product.id product_id
,count(id)*avg(product.amount) MRR  -- monthly recurring revenue
,avg(date_diff(date(expired_at), date(started_at), month)) avg_sub_life -- avg subscription lifespan
,count(id) avg_active_subs -- avg active subscriptions per month
from `holywater-test-task.solidtest.solid`
group by DATE_TRUNC(started_at, month), product.id)
group by product_id;

--5 :(

