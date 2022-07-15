create view maintenance.vw_discarded_rows
as  
select 
	dr.table_full_name,
	et.error_type_desc, 
	dr.error_type_id,
	dr.error_json,
	dr.error_count_rows,
	dr.mt_insert_dt
from maintenance.discarded_rows dr
left join maintenance.error_type et
	on et.error_type_id = dr.error_type_id
