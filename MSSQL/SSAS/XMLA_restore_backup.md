#### [Заметки по SSAS](./SSAS_note.md)  

### Развернуть Backup с помощью XMLA запроса

XMLA запрос

```xml
<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" Transaction="false">
  <Restore xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
    <File>E:\MSSQL\MSSQL14.MSSQLSERVER\MSSQL\OLAP\Backup\marketplace.abf</File>
    <DatabaseName>Marketplace</DatabaseName>
    <AllowOverwrite>true</AllowOverwrite>
  </Restore>
</Batch>
```


### Полезные ссылки:  

