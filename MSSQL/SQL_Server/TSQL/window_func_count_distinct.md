### [Заметки по T-SQL](./TSQL_note.md) 
## Подсчёт уникальных значений накопительным итогом
### Описание проблемы

Часто пишут как-то так

```sql
select count(distinct fo.customer_key) cnt_customer
	,dc.date_key
from fact_order fo
join dim_calendar dc
	on dc.date_key > fo.order_date_key
group by dc.date_key
```

В результате чего мы создаем датасет в котором на каждую дату мы берем копию таблицы фактов до этой даты. Датасет получается в разы больше самой таблицы фактов. А после мы делаем distinct, которому нужно отсортировать данные внутри каждой группы.

Время работы запроса написанного через оконные функции на порядок меньше.

```sql
with first_buy as
(
select 
	case when row_number() over (partition by customer_key order by order_date_key) = 1 then 1 else 0 end as dist_customer
	,order_date_key
from fact_order
), cnt_customer as
(
select
	sum(dist_customer) over (order by order_date_key, customer_key rows between unbounded preceding and current row) as cnt_customer
	,row_number() over (partition by order_date_key order by customer_key desc) rn
	,order_date_key
from first_buy 
where dist_customer = 1
)
select cnt_customer
	,order_date_key
from cnt_customer
where rn = 1
```

