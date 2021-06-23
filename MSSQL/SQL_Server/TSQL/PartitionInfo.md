# Информация по партициям таблицы

```sql
select
	sc.name + N'.' + so.name							 as [Schema.Table],
	si.index_id											 as [Index ID],
	si.type_desc										 as [Structure],
	si.name												 as [Index],
	stat.row_count										 as [Rows],
	stat.in_row_reserved_page_count * 8. / 1024. / 1024. as [In-Row GB],
	stat.lob_reserved_page_count * 8. / 1024. / 1024.	 as [LOB GB],
	p.partition_number									 as [Partition #],
	pf.name												 as [Partition Function],
	case pf.boundary_value_on_right
		when 1
			then 'Right / Lower'
		else 'Left / Upper'
	end													 as [Boundary Type],
	prv.value											 as [Boundary Point],
	fg.name												 as [Filegroup]
from
	sys.partition_functions as pf
	join sys.partition_schemes as ps on
		ps.function_id = pf.function_id
	join sys.indexes as si on
		si.data_space_id = ps.data_space_id
	join sys.objects as so on
		si.object_id = so.object_id
	join sys.schemas as sc on
		so.schema_id = sc.schema_id
	join sys.partitions as p on
		si.object_id = p.object_id and
		si.index_id = p.index_id
	left join sys.partition_range_values as prv on
		prv.function_id = pf.function_id and
		p.partition_number =
			case pf.boundary_value_on_right
				when 1
					then prv.boundary_id + 1
				else prv.boundary_id
			end
	/* For left-based functions, partition_number = boundary_id, 
       for right-based functions we need to add 1 */
	join sys.dm_db_partition_stats as stat on
		stat.object_id = p.object_id and
		stat.index_id = p.index_id and
		stat.index_id = p.index_id and
		stat.partition_id = p.partition_id and
		stat.partition_number = p.partition_number
	join sys.allocation_units as au on
		au.container_id = p.hobt_id and
		au.type_desc = 'IN_ROW_DATA'
	/* Avoiding double rows for columnstore indexes. */
	/* We can pick up LOB page count from partition_stats */
	join sys.filegroups as fg on
		fg.data_space_id = au.data_space_id
where
	so.object_id = object_id('core.item_snapshot')
order by
	[Schema.Table], [Index ID], [Partition Function], [Partition #]
```

