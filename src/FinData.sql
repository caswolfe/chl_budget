drop schema if exists FinData;
create schema FinData;
use FinData;

create table TransactionsEmn (
	acct varchar(64)
	,t_date date
    ,t_description varchar(256)
    ,amount decimal(10, 2)
    ,remaining decimal(10, 2)
);

create or replace view TransactionsEmnClean as (
	select
		t.acct
		,t.t_date
        ,t.t_description
        ,t.amount
        ,'' as c1_category
	from FinData.TransactionsEmn as t
	where
		t.t_description <> 'Starting Balance'
);

create table TransactionsC1 (
	card_no int
    ,t_date date
    ,posted_date date
    ,t_description varchar(256)
    ,category varchar(256)
    ,debit decimal(10, 2)
    ,credit decimal(10, 2)
);

create or replace view TransactionsC1Clean as (
	select
		concat('C1_', t.card_no) as acct
		,t.t_date
		,t.t_description
		,t.credit - t.debit as amount
		,t.category as c1_category
	from FinData.TransactionsC1 as t
);

create table Transactions (
	acct varchar(64)
    ,t_date date
    ,t_description varchar(256)
    ,amount decimal(10, 2)
    ,c1_category varchar(256)
);

create or replace view TransactionsExtra as (
	select
		acct
        ,t_date as "time"
        ,t_description as "description"
        ,amount
        ,c1_category
		,sum(t.amount) over (partition by t.acct order by t.t_date, t.amount) as rolling_acct_sum
		,sum(t.amount) over (order by t.t_date, t.amount) as rolling_total_sum
	from FinData.Transactions as t
);

delimiter //
create procedure update_transactions()
begin
	truncate table FinData.Transactions;
    insert into FinData.Transactions (acct, t_date, t_description, amount, c1_category)
        select acct,t_date,t_description,amount,c1_category from FinData.TransactionsEmnClean;
    insert into FinData.Transactions (acct, t_date, t_description, amount, c1_category)
        select acct,t_date,t_description,amount,c1_category from FinData.TransactionsC1Clean;
end//
delimiter ;

