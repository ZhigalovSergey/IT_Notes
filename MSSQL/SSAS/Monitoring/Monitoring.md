#### [Заметки по SSAS](../SSAS_note.md)  

### Мониторинг запросов к SSAS  

#### Описание проблемы

Когда у пользователей начинают подвисать запросы к кубам или падать по time out, возникает вопрос. Кто или что "подвешивает" сервер? И нужно исключить, что это пользователь с тяжелым запросом.

#### Решение  

- Можно посмотреть DMV для SSAS  

  ```sql
  -- Запрос MDX
  Select * from $System.Discover_Sessions
  Select * from $System.Discover_Connections
  Select * from $System.Discover_Commands
  
  -- Запрос SQL через Linked Server к SSAS
  select * from
  openquery([MARKETPLACE_OLAP], 'Select * from $System.Discover_Sessions')
  select * from
  openquery([MARKETPLACE_OLAP], 'Select * from $System.Discover_Connections')
  select * from
  openquery([MARKETPLACE_OLAP], 'Select * from $System.Discover_Commands')
  ```

  И если есть подозрение на какую-то сессию, убить её с помощью процедуры [ASSP](https://asstoredprocedures.github.io/functions/Cancel/) или XMLA запроса 

  ```sql
  -- Запрос MDX
  call assp.CancelSPID(99961)
  
  -- Запрос XMLA
  <Cancel xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
  <SPID>99961</SPID>
  <CancelAssociated>1</CancelAssociated>
  </Cancel>
  
  -- Запрос SQL через Linked Server к SSAS
  declare @xmla nvarchar(4000) 
  set @xmla = N'call assp.CancelSPID(99961)' 
  exec (@xmla) at [MARKETPLACE_OLAP]select * from
  openquery([MARKETPLACE_OLAP], 'Select * from $System.Discover_Connections')
  ```

  Также можно использовать другие DMV для анализа ситуации. Для [многомерной](./MS-SSAS.pdf) и [табулярной](./MS-SSAS-T.pdf) моделей они могут отличаться. Для удобства можно оборачивать запросы в SQL и работать с DataSets через Excel (накладывать фильтры и делать сортировки). Или как обычно, писать запросы по аналогии с SQL
  
  ```sql
  Select * 
  from $System.Discover_Sessions
  where SESSION_STATUS > 0
  order by SESSION_CPU_TIME_MS desc, SESSION_READ_KB desc
  ```
  
- Так как в DMV не хранится история запросов к серверу. То для более глубокого анализа можно настроить логирование запросов MDX. Есть два подхода, старый - [AsTrace](https://github.com/microsoft/Analysis-Services/tree/master/AsTrace) (по сути, это profiler без GUI) и новый - [XEvent](https://github.com/microsoft/Analysis-Services/tree/master/AsXEventSample)  

  Подход через Extended Events лучше, так как не оказывает дополнительной нагрузки на сервер.  Как правило, логирование через XEvents сохраняется в файл. Для извлечения данных из файла можно использовать запрос SQL, для примера: 
  
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
  
- Дополнительную информацию на основе DMV можно собрать с помощью проекта [ResMon](http://sqlsrvanalysissrvcs.codeplex.com/downloads/get/163669). Это куб, в который загружаются данные из DMV с определённым шагом.




#### Полезные ссылки:  

- [Dynamic XMLA using T-SQL for SQL Server Analysis Services](https://www.mssqltips.com/sqlservertip/2790/dynamic-xmla-using-tsql-for-sql-server-analysis-services/)  
- [SSAS - ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ](https://www.dvbi.ru/articles/reading/SSAS-optimization)  
- [ASSP - Analysis Services Stored Procedure Project](https://asstoredprocedures.github.io/ASStoredProcedures/)  
- [Мониторинг служб Analysis Services с помощью расширенных событий SQL Server](https://docs.microsoft.com/ru-ru/analysis-services/instances/monitor-analysis-services-with-sql-server-extended-events?view=asallproducts-allversions)  
- [Журнал операций в службах Analysis Services](https://docs.microsoft.com/ru-ru/analysis-services/instances/log-operations-in-analysis-services?view=asallproducts-allversions)  
- [Monitoring SQL Server Analysis Services with Extended Events](https://www.mssqltips.com/sqlservertip/6121/monitoring-sql-server-analysis-services-with-extended-events/)  
- [Using Extended Events to monitor DAX queries for SSAS Tabular Databases](https://www.mssqltips.com/sqlservertip/4548/using-extended-events-to-monitor-dax-queries-for-ssas-tabular-databases/)  
- [Performance Monitoring for SSAS – Extended Events Cheat Sheet](https://byobi.wordpress.com/2016/02/11/performance-monitoring-for-ssas-extended-events-cheat-sheet/)  
- [Extended Events for SSAS](https://codingsight.com/extended-events-for-ssas/)  
- [XMLA template script to configure Extended Events for Analysis Services](https://gist.github.com/brazilnut2000/a5e547635b4867d6e535)  

