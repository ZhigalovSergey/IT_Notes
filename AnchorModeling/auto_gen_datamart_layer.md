### [AnchorModeling](./AnchorModeling.md)

## Автоматизация разработки слоя datamart

### Описание проблемы

Для написания процедуры синхронизации под каждое поле сущности уходит довольно много времени при этом код достаточно простой и на первый взгляд поддается автогенерированию. В отношении ETL пакетов ситуация аналогична.

### Варианты решения 

Для начала напишем command-line приложение, на вход которому будем давать JSON файла (metadata.json), в котором будет вся информация, необходимая для генерирования слоя. А на выходе получать код таблиц и процедур в слоя datamart.

### Реализация

Создадим шаблоны для **DataMart** и разместим в папке **template**. 

Шаблон для [таблицы DataMart](./template/dbo/tbl/datamart.sql)

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

Так как витрина может быть основана на нескольких сущностях, то сведем задачу получения полей (#attrs#) к написанию запроса SQL, где будут перечислены все поля витрины. Результат этого запроса сохраним в файле **metadata_tbl_columns.sql** в папке metadata и укажем об этом в **metadata.json**. Также список полей можно занести в файл вручную.

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+'[#'+c.name+'#] '+t.name++case when t.name = 'nvarchar' then '('+cast(c.max_length/2 as nvarchar)+')' else '' end+','+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'collections'
and c.name not in ('mt_insert_dt','mt_update_dt','mt_delete_dt')

select @sql
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

В шаблоне **datamart_sync.sql** возникает аналогичная задача в получении списка поле. Её можно решать с помощью запроса SQL, немного его модифицировав. Или же заполнить файл **metadata_proc_columns_list.sql (используется для #list#)** руками.

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+char(9)+'[#'+c.name+'#],'+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'collections'
and c.name not in ('mt_insert_dt','mt_update_dt','mt_delete_dt')

select @sql
```

Аналогичный запрос для файла **metadata_proc_columns_init.sql (используется для #init#)**

```sql
use [MDWH_RAW]
go

declare @sql nvarchar(max) = ''

select @sql = @sql + char(9)+char(9)+'null as [#'+c.name+'#],'+char(13)
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
inner join sys.types t on c.user_type_id = t.user_type_id
where o.name = 'collections'
and c.name not in ('mt_insert_dt','mt_update_dt','mt_delete_dt')

select @sql
```

Как видим, это по сути один и тот же запрос. Разница только в формировании поля.

Шаблон для **business_key** требует дополнительной доработки на стороне C# в случае если у нас составной бизнес ключ. 

```sql
	update tgt
	set
		#set_attrs# --tgt.[#anchor#_#attr_bk#] = src.[#anchor#_#attr_bk#],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_s_#src_name# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists 
			#where_attrs# --(select tgt.#anchor#_#attr_bk# intersect select src.#anchor#_#attr_bk#)

	execute maintenance.InsertExecution
		@Step = N'update #attr_bk#',
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

Перейдем собственно к написанию command-line приложения, которое будет генерировать слой datamart.  

```c#
Console.WriteLine(args[0]);
string dir = args[0];

string text;
string fl_new;

string fl_json = dir + "\\metadata.json";
string json = File.ReadAllText(fl_json);

JObject mt = JObject.Parse(json);

JToken mapping = mt.SelectToken("$.mapping");
Console.WriteLine("mapping is: " + mapping);
Dictionary<string, string> dict_attr = JsonConvert.DeserializeObject<Dictionary<string, string>>(mapping.ToString());

string[] bk = mt.SelectToken("$.raw_table.business_key").Select(s => (string)s).ToArray();
Console.WriteLine("bk is : " + String.Join("; ", bk));

string attr_bk = bk[0];

string anchor = (string)mt.SelectToken("$.anchor");
Console.WriteLine("anchor is : " + anchor);

string src_name = (string)mt.SelectToken("$.src_name");
Console.WriteLine("src_name is : " + src_name);
Console.ReadLine();

string tbl_path = dir + "\\test\\dbo\\tbl\\";
if (!Directory.Exists(tbl_path))
{
	Directory.CreateDirectory(tbl_path);
}

string proc_path = dir + "\\test\\dbo\\proc\\";
if (!Directory.Exists(proc_path))
{
	Directory.CreateDirectory(proc_path);
}

// create datamart.sql
text = File.ReadAllText(dir + "\\template\\dbo\\tbl\\datamart.sql");
fl_new = string.Format(dir + "\\test\\dbo\\tbl\\" + anchor + ".sql");
text = text.Replace("#anchor#", anchor);
string tbl_columns = File.ReadAllText(dir + "\\metadata\\metadata_tbl_columns.sql");
string src_attr;
string attr;
foreach (KeyValuePair<string, string> kvp in dict_attr)
{
	src_attr = kvp.Key;
	attr = kvp.Value;

	tbl_columns = tbl_columns.Replace("#" + src_attr + "#", anchor + "_" + attr);
}
text = text.Replace("#attrs#", tbl_columns);
File.WriteAllText(fl_new, text);

// create datamart_sync.sql
text = File.ReadAllText(dir + "\\template\\dbo\\proc\\datamart_sync.sql");
fl_new = string.Format(dir + "\\test\\dbo\\proc\\" + anchor + "_sync.sql");
text = text.Replace("#anchor#", anchor);

string list = File.ReadAllText(dir + "\\metadata\\metadata_proc_columns_list.sql");
string init = File.ReadAllText(dir + "\\metadata\\metadata_proc_columns_init.sql");
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
upd_bk = upd_bk.Replace("#attr_bk#", attr_bk);
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