#### [Заметки по Extended Events](./ExtendedEvents_note.md)  

### Парсинг xel файлов

Логирование MDX запросов через ExtEvents
Можно смотреть через запрос

```sql
select
    n.value('(data[@name="StartTime"]/value)[1]', 'datetime2') as StartTime,
    n.value('(data[@name="EndTime"]/value)[1]', 'datetime2') as EndTime,
    n.value('(data[@name="ServerName"]/value)[1]', 'nvarchar(255)') as ServerName,
    n.value('(data[@name="DatabaseName"]/value)[1]', 'nvarchar(255)') as DatabaseName,
    n.value('(data[@name="NTUserName"]/value)[1]', 'nvarchar(255)') as UserName,
    n.value('(data[@name="TextData"]/value)[1]', 'nvarchar(max)') as MDXQuery
from (select cast(event_data as XML) as event_data
from sys.fn_xe_file_target_read_file('\\mir-sdb-008\ExtEventsLog\MDX_Traces*.xel', null, null, null)) ed
cross apply ed.event_data.nodes('event') as q(n)
```

