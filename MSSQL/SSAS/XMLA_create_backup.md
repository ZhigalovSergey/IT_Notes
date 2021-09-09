#### [Заметки по SSAS](../SSAS_note.md)  

### Создать Backup с помощью XMLA запроса

XMLA запрос

```xml
<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" Transaction="false">
  <Backup xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
    <Object>
      <DatabaseID>Marketplace</DatabaseID>
    </Object>
    <File>E:\MSSQL\MSAS13.MARKETPLACE\OLAP\Backup\Marketplace.abf</File>
    <AllowOverwrite>true</AllowOverwrite>
    <ApplyCompression>true</ApplyCompression>
  </Backup>
</Batch>
```


### Полезные ссылки:  

