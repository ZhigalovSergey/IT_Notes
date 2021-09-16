#### [Заметки по SSAS](./SSAS_note.md)  

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

  Также можно использовать другие DMV для анализа ситуации. Для [многомерной](./MS-SSAS.pdf) и [табулярной](./MS-SSAS-T.pdf) моделей они могут отличаться.




#### Полезные ссылки:  

- [Dynamic XMLA using T-SQL for SQL Server Analysis Services](https://www.mssqltips.com/sqlservertip/2790/dynamic-xmla-using-tsql-for-sql-server-analysis-services/)  
- 
