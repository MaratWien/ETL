DELETE FROM mart.f_customer_retention
WHERE f_customer_retention.period_id IN (
    SELECT substr(d_calendar.week_of_year_iso, 1, 8)
    FROM mart.d_calendar
    WHERE d_calendar.date_actual < '{{ds}}'
    AND d_calendar.date_actual >= '2022-01-01'
);
INSERT INTO mart.f_customer_retention
   (period_id, period_name, item_id,
    new_customers_count, new_customers_revenue,
	returning_customers_count, returning_customers_revenue,
	refunded_customer_count, customers_refunded)
SELECT week_of_year, 'weekly' period_name, item_id,
    count(case when shipped_count = 1 then 1 end) cust_ord_cnt_eq_1,
	sum(case when shipped_count = 1 then shipped_pmt else 0 end) cust_pmt_ord_cnt_eq_1,
	count(case when shipped_count > 1 then 1 end) cust_ord_cnt_more_1,
	sum(case when shipped_count >1 then shipped_pmt else 0 end) cust_pmt_ord_cnt_more_1,
	count(case when refunded_count >0 then 1 end) cust_cnt_refund,
	sum(case when refunded_count >0 then refunded_qty else 0 end) cust_qty_refund
FROM (
    select item_id as item_id,
            substr(d_calendar.week_of_year_iso, 1, 8) week_of_year,
            uol2.customer_id,
            count(CASE WHEN status = 'shipped' THEN 1 END) shipped_count,
	        sum(CASE WHEN status = 'shipped' THEN uol2.payment_amount END) shipped_pmt,
	        count(CASE WHEN status = 'refunded' THEN 1 END) refunded_count,
	        sum(CASE WHEN status = 'refunded' THEN uol2.quantity  END) refunded_qty
	FROM staging.user_order_log uol2
	JOIN mart.d_calendar ON uol2.date_time::DATE = d_calendar.date_actual
	WHERE uol2.date_time::DATE < '{{ ds }}'
	GROUP BY item_id, substr(d_calendar.week_of_year_iso, 1, 8), customer_id
) t
GROUP BY item_id, week_of_year;