ALTER TABLE staging.user_order_log add column status varchar(30) default 'shipped' NOT NULL;