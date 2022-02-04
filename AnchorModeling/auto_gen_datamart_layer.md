### [AnchorModeling](./AnchorModeling.md)

## Автоматизация разработки слоя datamart

### Описание проблемы

Для написания процедуры синхронизации под каждое поле сущности уходит довольно много времени при этом код достаточно простой и на первый взгляд поддается автогенерированию. В отношении ETL пакетов ситуация аналогична.

### Варианты решения 

Для начала напишем command-line приложение, на вход которому будем давать JSON файла (metadata.json), в котором будет вся информация, необходимая для генерирования слоя. А на выходе получать код таблиц и процедур в слоя datamart.

### Реализация

Создадим шаблоны для **DataMart**. Разместим шаблоны в папке **template**. 

Шаблон для [таблицы Anchor](./template/tbl/anchor.sql)

```sql
create table [dbo].[#anchor#] (
	[#anchor#_key] [bigint] not null,
#attrs#
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk$#anchor#] primary key nonclustered
	(
	[#anchor#_key] asc
	) on [DATA_MART]
) on [DATA_MART]
go
```

Шаблон для **процедуры синхронизации DataMart** - в разработке, нужно продумать дополнительные поля для таблицы источников anchor, где будут храниться имена источников в сокращенном виде. Также продумать хранение полей бизнес-ключа для разных источников. Например, в одной системе бизнес ключ может быть id int not null, а в другом набор полей типа nvarchar(255). Также при наличии нескольких источников процедура усложняется. Сейчас шаблон имеет следующий вид

```sql
create procedure [dbo].[#anchor#_sync]
as begin
	set nocount on;

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)

	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid

	execute maintenance.InsertExecution
		@Step = N'start dbo.#anchor#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	insert into dbo.#anchor# (
		[#anchor#_key],
#list#
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		src.#anchor#_key,
#init#
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt
	from
		core.#anchor# as src
	where
		src.mt_insert_dt >= @load_dt and
		not exists (
			select
				1
			from
				dbo.#anchor# as tgt
			where
				tgt.#anchor#_key = src.#anchor#_key
		)

	execute maintenance.InsertExecution
		@Step = N'insert into dbo.#anchor#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

#upd_business_key#
	
#upd_attrs#	
	
end
	go
```

Дополнительно имеем шаблон для **business_key**

```sql
	update tgt
	set
		tgt.[#anchor#_#attr_bk_1#] = src.[#anchor#_#attr_bk_1#],
		tgt.[#anchor#_#attr_bk_2#] = src.[#anchor#_#attr_bk_2#],
		tgt.[#anchor#_#attr_bk_3#] = src.[#anchor#_#attr_bk_3#],
		tgt.[#anchor#_#attr_bk_4#] = src.[#anchor#_#attr_bk_4#],
		tgt.[#anchor#_#attr_bk_5#] = src.[#anchor#_#attr_bk_5#],
		tgt.[#anchor#_#attr_bk_6#] = src.[#anchor#_#attr_bk_6#],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_s_#src_name# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (
				isnull(tgt.[#anchor#_#attr_bk_1#], '||') = isnull(src.[#anchor#_#attr_bk_1#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_2#], '||') = isnull(src.[#anchor#_#attr_bk_2#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_3#], '||') = isnull(src.[#anchor#_#attr_bk_3#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_4#], '||') = isnull(src.[#anchor#_#attr_bk_4#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_5#], '||') = isnull(src.[#anchor#_#attr_bk_5#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_6#], '||') = isnull(src.[#anchor#_#attr_bk_6#], '||')
			)

	execute maintenance.InsertExecution
		@Step = N'update #attr_bk_1#, #attr_bk_2#, #attr_bk_3#, #attr_bk_4#, #attr_bk_5#, #attr_bk_6#)',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
```

И шаблон для **attrs**

```sql
	update tgt
	set
		tgt.#anchor#_#attr# = src.#anchor#_#attr#,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_x_#attr# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt = @load_dt
		and not (isnull(tgt.#anchor#_#attr#, '') = isnull(src.#anchor#_#attr#, ''))

	execute maintenance.InsertExecution
		@Step = N'update #anchor#_#attr#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
		
```

Запрос для извлечения полей таблицы. Результат этого запроса сохраним в **columns.sql**, рядом с шаблоном **datamart.sql**

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+'[#'+c.name+'#] '+t.name+'('+cast(c.max_length/2 as nvarchar)+'),'+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'dimUtmExtended'
and c.name not in ('mt_insert_dt','mt_update_dt','update_dt')

select @sql
```

Для шаблона **datamart_sync.sql** запустим запрос и сохраним результат в **list.sql**

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+char(9)+'[#'+c.name+'#],'+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'dimUtmExtended'
and c.name not in ('mt_insert_dt','mt_update_dt','update_dt')

select @sql
```

И запустим запрос и сохраним результат в **init.sql**

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+char(9)+'null as [#'+c.name+'#],'+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'dimUtmExtended'
and c.name not in ('mt_insert_dt','mt_update_dt','update_dt')

select @sql
```

Перейдем собственно к написанию command-line приложения, которое будет генерировать слой datamart. Для этого допишем код для генерации слоя core 

```c#
// create datamart_sync.sql
text = File.ReadAllText(dir + "\\template\\dbo\\proc\\datamart_sync.sql");
fl_new = string.Format(dir + "\\test\\dbo\\proc\\" + anchor + ".sql");
text = text.Replace("#anchor#", anchor);

string list = File.ReadAllText(dir + "\\template\\dbo\\proc\\list.sql");
string init = File.ReadAllText(dir + "\\template\\dbo\\proc\\init.sql");
foreach (KeyValuePair<string, string> kvp in dict_attr)
{
    src_attr = kvp.Key;
    attr = kvp.Value;

    list = list.Replace("#" + src_attr + "#", anchor + "_" + attr);
    init = init.Replace("#" + src_attr + "#", anchor + "_" + attr);
}
text = text.Replace("#list#", list);
text = text.Replace("#init#", init);

// update attribute_business_key
string upd_bk = File.ReadAllText(dir + "\\template\\dbo\\proc\\update_business_key.sql");
upd_bk = upd_bk.Replace("#anchor#", anchor);
upd_bk = upd_bk.Replace("#src_name#", src_name);
upd_bk = upd_bk.Replace("#attr_bk_1#", attr_bk_1);
upd_bk = upd_bk.Replace("#attr_bk_2#", attr_bk_2);
upd_bk = upd_bk.Replace("#attr_bk_3#", attr_bk_3);
upd_bk = upd_bk.Replace("#attr_bk_4#", attr_bk_4);
upd_bk = upd_bk.Replace("#attr_bk_5#", attr_bk_5);
upd_bk = upd_bk.Replace("#attr_bk_6#", attr_bk_6);
text = text.Replace("#upd_business_key#", upd_bk);

// update attributes
string upd_attrs = "";
foreach (KeyValuePair<string, string> kvp in dict_attr)
{
    if (bk.Contains(kvp.Key)) continue;
    src_attr = kvp.Key;
    attr = kvp.Value;
    string upd_attr = File.ReadAllText(dir + "\\template\\dbo\\proc\\update_attr.sql");

    upd_attr = upd_attr.Replace("#anchor#", anchor);
    upd_attr = upd_attr.Replace("#attr#", attr);
    upd_attrs += upd_attr;
}

text = text.Replace("#upd_attrs#", upd_attrs);
File.WriteAllText(fl_new, text);
```

Также нужно сгенерировать ETL пакет.  

### Полезные ссылки

- [JSON](https://ru.wikipedia.org/wiki/JSON)