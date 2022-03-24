create view maintenance_idx.index_stat_operations as
with
LAST_OPERATIONS as (
	select top (1) with ties
		ql.dbase_name,
		json_value(ql.obj_identifier, '$."schema_name"')	  as [schema_name],
		json_value(ql.obj_identifier, '$."table_name"')		  as table_name,
		json_value(ql.obj_identifier, '$."index_name"')		  as index_name,
		json_value(ql.obj_identifier, '$."partition_number"') as partition_number,
		ql.start_dt,
		ql.finish_dt,
		ql.comment,
		ql.cmd_SQL

	from
		queue_sql.queue_list as ql
	where
		ql.mt_insert_dt >= getdate() - 10 and
		ql.start_dt is not null
	order by
		row_number() over (partition by ql.dbase_name, ql.obj_name, ql.obj_identifier order by ql.start_dt desc)
)
select
	st.dbase_name,
	st.sch_name + '.' + st.table_name										   as table_name,
	st.index_name,
	st.partition_number,
	st.index_type,
	st.size_MB,
	st.row_count,
	st.fragmentation,
	st.mt_insert_dt,
	st.mt_update_dt,
	o.start_dt																   as last_oper_start,
	o.finish_dt																   as last_oper_finish,
	cast(cast(o.finish_dt as datetime) - cast(o.start_dt as datetime) as time) as last_oper_time,
	o.comment																   as last_oper_result,
	o.cmd_SQL																   as last_oper_cmd_SQL
from
	maintenance_idx.index_stat as st
	left join LAST_OPERATIONS as o on
		st.dbase_name = o.dbase_name and
		st.sch_name = o.[schema_name] and
		st.table_name = o.table_name and
		st.index_name = o.index_name and
		st.partition_number = o.partition_number
where
	st.row_count > 0