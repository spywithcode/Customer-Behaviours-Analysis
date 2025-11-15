use customer_behaviours;

select * from customer limit 20;

-- Q1. Which coustomer use a discount but still spend more than the average purchase amount -- 
select gender, SUM(purchase_amount) as revenue
from customer
group by gender;

-- Q2. Which coustomer use a discount but still spend more than the average purchase amount -- 
select customer_id , purchase_amount
from customer
where discount_applied = 'Yes' and purchase_amount >= (select avg(purchase_amount) from customer);

-- Q3. Which are the top 5 products with the highest avaerage review rating --
select item_purchased, ROUND(AVG(review_rating),2) as "Average Product Rating"
from customer
group by item_purchased
order by avg(review_rating) desc
limit 5;

-- Q4. Compare the average Purchase Amounts between Standard and Express Shipping --
select shipping_type, round(avg(purchase_amount),2)
from customer
where shipping_type in ('Standard','Express')
group by shipping_type;

-- Q5. Do subcribed customers spend more? Comapre average spend and total revenue 
-- between subcribers and non-subcribers.
select subscription_status,
count(customer_id) as total_customer,
round(avg(purchase_amount),2) as avg_spend,
round(sum(purchase_amount),2) as total_revenue
from customer
group by subscription_status
order by total_revenue, avg_spend desc;

-- Q6. Which 5 products have the highest percentage of purchases with discounts applied?
select item_purchased,
round(100 * sum(case when discount_applied = 'yes' then 1 else 0 end)/count(*),2) as discount_rate
from customer
group by item_purchased
order by discount_rate desc
limit 5;

-- Q7. Segment customer into New, Returning, and loyal based on their total number of previous purcheses, 
-- and show the count of each segment.
with customer_type as (
select customer_id, previous_purchases,
case
	when previous_purchases = 1 then 'New'
    when previous_purchases between 2 and 10 then 'Returning'
    else 'Loyal'
    end as customer_segment
from customer
)

select customer_segment, count(*) as "Number of Customers"
from customer_type
group by customer_segment;

-- Q8 What are the top 3 most purchases products with in each category?
with item_counts as(
select category, item_purchased, count(customer_id) as total_orders,
ROW_NUMBER() over(partition by category order by count(customer_id)desc) as item_rank
from customer
group by category, item_purchased
)

select item_rank, category, item_purchased, total_orders
from item_counts
where item_rank <= 3;

-- Q9. Are customers who are repeat buyers (more than 5 previous purchases) also likely to subscribe?
select subscription_status, 
count(customer_id) as repeat_buyers
from customer
where previous_purchases > 5
group by subscription_status;

-- Q10. What is the revenue contribution of each age group?
select age_group, sum(purchase_amount) as total_revenue
from customer
group by age_group
order by total_revenue desc;













