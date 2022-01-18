## Автоматизация разработки слоя core

### Описание проблемы

Для написания процедур и таблиц под каждое поле сущности уходит довольно много времени при этом код достаточно простой и на первый взгляд поддается автогенерированию.

### Варианты решения 

Для начала напишем command-line приложение, на вход которому будем давать список полей из таблицы слоя raw. А на выходе получать код таблиц и процедур в слое core. В дальнейшем продумать структуру JSON файла (metadata.json), в котором будет вся информация, необходимая для генерирования слоя core.

### Реализация

Создадим шаблоны для **Anchor** и **Attribute**. Разместим шаблоны в папке **template**. 

Шаблон для [таблицы Anchor](./template/tbl/anchor.sql)

```sql
create table [core].[#anchor#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_source_key] [tinyint] not null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_#anchor#] primary key clustered
	(
	[#anchor#_key]
	) on [core]
) on [core]
go

create nonclustered index [mt_update_dt] on [core].[#anchor#]
(
[mt_insert_dt]
)
go
```

Шаблон для [таблицы источников Anchor](./template/tbl/anchor_source.sql)

```sql
create table [core].[#anchor#_source] (
	[#anchor#_source_key] [tinyint] not null,
	[#anchor#_source_name] [nvarchar](255) not null,
	constraint [pk_#anchor#_source] primary key clustered
	(
	[#anchor#_source_key] asc
	) on [core]
) on [core]
go
```

Шаблон для **процедуры синхронизации Anchor** - в разработке, нужно продумать дополнительные поля для таблицы источников anchor, где будут храниться имена источников в сокращенном виде. Также продумать хранение полей бизнес-ключа для разных источников. Например, в одной системе бизнес ключ может быть id int not null, а в другом набор полей типа nvarchar(255). Также при наличии нескольких источников процедура усложняется. Нужно продумать, в каком виде эта информация будет передаваться на вход приложению, возможно в виде JSON-файла. 

Шаблон для **таблицы Attribute**

```sql
create table [core].[#anchor#_x_#attr#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_#attr#] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_#anchor#_#attr#] primary key clustered
	(
	[#anchor#_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[#anchor#_x_#attr#]
(
[mt_insert_dt]
)
go
```

Шаблон для **таблицы Attribute бизнес ключа**

```sql
create table [core].[#anchor#_s_#attr_bk#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_#attr_bk#] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_#anchor#_#attr_bk#] primary key clustered
	(
	[#anchor#_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[#anchor#_s_#attr_bk#]
(
[mt_insert_dt]
)
go
```

Шаблон для **процедуры синхронизации Attribute** - в разработке, нужно продумать указание таблицы Attribute бизнес ключа и соответствие между названиями полей слоя raw и названиями атрибутов.

Шаблон для генерирования **Sequence**

```sql
create sequence [core].[#anchor#_sequence]
as bigint
start with 0
increment by 1
go
```

**Шаблоны не учитывают типы данных атрибутов**, что следует доработать. И брать информацию из системных таблиц, получая на вход название таблицы в слое raw

Запишем список полей в файл list.txt. 

В дальнейшем продумать структуру JSON файла (metadata.json), в котором будет вся информация, необходимая для генерирования слоя core.

Запрос для извлечения полей таблицы

```sql
use [MDWH_RAW]
go

select c.name, *
from sys.objects o
inner join sys.columns c on o.object_id = c.object_id
where o.name = 'dimUtmExtended'
```

Перейдем собственно к написанию command-line приложения, которое будет генерировать слой core.

```c#
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace gen_core_layer
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine(args[0]);
            string dir = args[0];

            string text;
            string anchor = "utm_extended";
            string fl_new;

            // create anchor
            text = File.ReadAllText(dir + "\\template\\tbl\\anchor.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + ".sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_source.sql
            text = File.ReadAllText(dir + "\\template\\tbl\\anchor_source_ref.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_source_ref.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_sequence.sql
            text = File.ReadAllText(dir + "\\template\\seq\\anchor_sequence.sql");
            fl_new = string.Format(dir + "\\test\\seq\\" + anchor + "_sequence.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            string src_name = "gbq";
            // "id", "source", "medium", "campaign", "content", "term"
            string attr_bk_1 = "hash";
            string attr_bk_2 = "source";
            string attr_bk_3 = "medium";
            string attr_bk_4 = "campaign";
            string attr_bk_5 = "content";
            string attr_bk_6 = "term";

            // attribute_business_key.sql
            text = File.ReadAllText(dir + "\\template\\tbl\\attribute_business_key.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_s_" + src_name + ".sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            text = text.Replace("#attr_bk_1#", attr_bk_1);
            text = text.Replace("#attr_bk_2#", attr_bk_2);
            text = text.Replace("#attr_bk_3#", attr_bk_3);
            text = text.Replace("#attr_bk_4#", attr_bk_4);
            text = text.Replace("#attr_bk_5#", attr_bk_5);
            text = text.Replace("#attr_bk_6#", attr_bk_6);
            File.WriteAllText(fl_new, text);

            // anchor_sync.sql
            text = File.ReadAllText(dir + "\\template\\proc\\anchor_sync.sql");
            fl_new = string.Format(dir + "\\test\\proc\\" + anchor + "_sync.sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            File.WriteAllText(fl_new, text);

            //string fl_list = dir + "\\list.txt";
            //StreamReader reading = File.OpenText(fl_list);

            string fl_json = dir + "\\metadata.json";
            string json = File.ReadAllText(fl_json);

            JObject mt = JObject.Parse(json);
            JToken mapping = mt.SelectToken("$.mapping");
            string[] bk = mt.SelectToken("$.raw_table.business_key").Select(s => (string)s).ToArray();
            Console.WriteLine(mapping);
            Console.ReadLine();
            Dictionary<string, string> dict_attr = JsonConvert.DeserializeObject<Dictionary<string, string>>(mapping.ToString());
            string src_attr;
            string attr;

            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                if (bk.Contains(kvp.Key)) continue;
                Console.WriteLine("src_attr = {0}, attr = {1}", kvp.Key, kvp.Value);
                src_attr = kvp.Key;
                attr = kvp.Value;

                text = File.ReadAllText(dir + "\\template\\tbl\\attribute.sql");
                fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_x_" + attr + ".sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#src_name#", src_name);
                File.WriteAllText(fl_new, text);

                text = File.ReadAllText(dir + "\\template\\proc\\attribute_sync.sql");
                fl_new = string.Format(dir + "\\test\\proc\\" + anchor + "_x_" + attr + "_sync.sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#src_name#", src_name);
                text = text.Replace("#src_attr#", src_attr);
                File.WriteAllText(fl_new, text);
            }

            Console.ReadLine();
        }
    }
}
```

Также нужно сгенерировать скрипты миграции. 

### Полезные ссылки

- [JSON](https://ru.wikipedia.org/wiki/JSON)