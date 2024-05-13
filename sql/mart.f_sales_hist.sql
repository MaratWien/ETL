DELETE FROM mart.f_sales
WHERE f_sales.date_id IN(
    SELECT d_calendar.date_id
    FROM mart.d_calendar
    WHERE mart.d_calendar.date_actual < '{{ds}}'
);
INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
SELECT dc.date_id,
        item_id,
        customer_id,
        city_id,
        quantity,
        payment_amount
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar AS dc ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE < '{{ds}}';