# ETL. Модификация витрины данных.

## Задача

* Модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса.
* Обеспечить обратную совместимость.
* Создать обновленную витрину данных для исследования возвращаемости клиентов.
* Модифицировать ETL процесс, для поддержки идемпотентности.

Изменения и создания объектов БД, миграции данных в новую структуру [sql](./sql/)
Основной скрипт для Airflow [DAG](./src/dag.py)

## Модификация в пайплайне

Добавляю новое поле `status` в таблицу `staging.user_order_log`
В старые записи добавил статус `shiped`

Скрипт [1_add_staging_uol_field_status.sql](./sql/1_add_staging_uol_field_status.sql)
```sql
ALTER TABLE staging.user_order_log
ADD COLUMN status varchar(30) DEFAULT 'shipped' NOT NULL;
```

Добавил в витрину `mart.f_sales` новое поле `refund_flag` типа `boolean` для контроля заполнения [2_add_mart_f_sales_field_refund_flag.sql](./sql/2_add_mart_f_sales_field_refund_flag.sql)
```sql
ALTER TABLE mart.f_sales
ADD COLUMN refund_flag boolean DEFAULT false NOT NULL;
```
Чтобы данные можно было обновлять за выборочные дни без удаления и дублирования. Так как данные за конкретную дату грузятся только этим процессом и не агрегируются в витрине, то нарушений не будет.

Для инкрементов и исторических данных скрипт будет немного различаться, так как в первом случае грузятся данные только за одну дату (условная дата запуска - ds), во втором - за произвольные даты до даты ds.

Скрипт [mart.f_sales_inc.sql](./sql/mart.f_sales_inc.sql)
```sql
DELETE FROM mart.f_sales
WHERE f_sales.date_id IN(
    SELECT d_calendar.date_id
    FROM mart.d_calendar
    WHERE mart.d_calendar.date_actual = '{{ds}}'
);

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, refund_flag)
SELECT dc.date_id,
        item_id,
        customer_id,
        city_id,
        quantity * (CASE WHEN uol.status = 'refunded' THEN -1 ELSE 1 END) quantity,
        payment_amount * (CASE WHEN uol.status = 'refunded' THEN -1 ELSE 1 END) payment_amount,
        (uol.status = 'refunded')
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar AS dc ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE = '{{ds}}';
```

Скрипт для исторических данных [mart.f_sales_hist.sql](./sql/mart.f_sales_hist.sql)
```sql
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
```

В файле настройки DAG создал для загрузки исторических данных второй DAG, в котором создал свой набор задач (с префиксом h_).

```python
update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_inc.sql",
        parameters={"date": {business_dt}})
```

Запрос удаления ранее загруженных данных за эту дату в таблице `user_order_log`:
```python
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date =< '{date}'"
engine.execute(str_del)
```

Создал витрину по требуемой структуре. Сделал внешний ключ по `item_id` к `mart.d_item`.[f_customer_retention](./sql/3_create_f_customer_retention.sql)
```sql
DROP TABLE IF EXISTS mart.f_customer_retention ;
CREATE TABLE mart.f_customer_retention (
	id serial4 PRIMARY KEY,
    new_customers_count int4 not null,
    returning_customers_count int4 not null,
    refunded_customer_count int4 not null,
    period_name varchar(20) not null,
    period_id varchar(20) not null,
    item_id int4 not null,
    new_customers_revenue numeric(12,2) not null,
    returning_customers_revenue numeric(12,2) not null,
    customers_refunded numeric(12,0) not null,

    CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id)
        REFERENCES mart.d_item (item_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION);

	CREATE INDEX IF NOT EXISTS f_cr2
    ON mart.f_customer_retention USING btree
    (item_id ASC NULLS LAST)
    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS f_cr3
    ON mart.f_customer_retention USING btree
    (period_id ASC NULLS LAST)
    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS f_cr4
    ON mart.f_customer_retention USING btree
    (period_name ASC NULLS LAST)
    TABLESPACE pg_default;
```

Для заполнения по историческим данным [mart.f_customer_retention_hist.sql](./sql/mart.f_customer_retention_hist.sql)

В DAG добавляю соответствующие задачи типа `PostgresOperator`:
```python
h_update_f_customer_retention = PostgresOperator(
    task_id='update_f_customer_retention',
    postgres_conn_id=postgres_conn_id,
    sql="sql/mart.f_customer_retention_hist.sql",
    parameters={"date": {business_dt}} )

update_f_customer_retention = PostgresOperator(
    task_id='update_f_customer_retention',
    postgres_conn_id=postgres_conn_id,
    sql="sql/mart.f_customer_retention_inc.sql",
    parameters={"date": {business_dt}} )
```

В процессе были реализованы процедуры очистки и заполнения таблиц таким образом, чтобы было возможно независимое удаление и восстановление информации за отдельные дни без затрагивания информации за другие дни.

В частности изменили работу функций `f_upload_data_to_staging`, `f_upload_data_to_staging_hist` в [DAG](./src/dag.py), чтобы можно было обновлять за выборочные дни без удаления и дублирования: выполняется запрос удаления ранее загруженных данных за эту дату в таблице `user_order_log`

Для инкрементов:
```python
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'"
engine.execute(str_del)
```
Для исторических данных:
```python
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date < '{date}'"
engine.execute(str_del)
```
Для витрины `mart.f_customer_retention` выполняется удаление данных за соответствующую неделю
```sql
DELETE FROM mart.f_customer_retention
WHERE f_customer_retention.period_id = (
    SELECT substr(d_calendar.week_of_year_iso, 1, 8)
    FROM mart.d_calendar
    WHERE d_calendar.date_actual = '{{ds}}'
);
```