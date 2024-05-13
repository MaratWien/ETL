DELETE FROM mart.f_customer_retention
WHERE f_customer_retention.period_id = (
    SELECT substr(d_calendar.week_of_year_iso, 1, 8)
    FROM mart.d_calendar
    WHERE d_calendar.date_actual = '{{ds}}'
);

INSERT INTO mart.f_customer_retention(
    period_id, period_name, item_id,
    new_customers_count, new_customers_revenue,
	returning_customers_count, returning_customers_revenue,
	refunded_customer_count, customers_refunded)
SELECT week_of_year, 'weekly' period_name, item_id,
        count(CASE WHEN shipped_count = 1 THEN 1 END) cust_ord_cnt_eq_1,
	    sum(CASE WHEN shipped_count = 1 THEN shipped_pmt ELSE 0 END) cust_pmt_ord_cnt_eq_1,
	    count(CASE WHEN shipped_count > 1 THEN 1 END) cust_ord_cnt_more_1,
	    sum(CASE WHEN shipped_count > 1 THEN shipped_pmt ELSE 0 END) cust_pmt_ord_cnt_more_1,
	    count(CASE WHEN refunded_count > 0 THEN 1 END) cust_cnt_refund,
	    sum(CASE WHEN refunded_count > 0 THEN refunded_qty ELSE 0 END) cust_qty_refund
FROM (
    SELECT item_id AS item_id,
            substr(d_calendar.week_of_year_iso, 1, 8) week_of_year,
            uol2.customer_id, count(CASE WHEN status = 'shipped' THEN 1 END) shipped_count,
	        sum(CASE THEN status = 'shipped' THEN uol2.payment_amount END) shipped_pmt,
	        count(CASE WHEN status = 'refunded' THEN 1 END) refunded_count,
	        sum(CASE WHEN status = 'refunded' THEN uol2.quantity END) refunded_qty
	FROM staging.user_order_log uol2
	JOIN mart.d_calendar ON uol2.date_time::DATE = d_calendar.date_actual
			AND '{{ ds }}' BETWEEN d_calendar.first_day_of_week AND d_calendar.last_day_of_week
	GROUP BY item_id, substr(d_calendar.week_of_year_iso, 1, 8), customer_id
) t
GROUP BY item_id, week_of_year