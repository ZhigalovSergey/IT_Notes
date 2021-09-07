#### [Заметки по SSAS](../SSAS_note.md)  

### Поиск ошибки при процессинге измерений куба  

Иногда возникают ошибки из-за изменения размера полей или даже типа данных на стороне реляционного хранилища.

> dimensions:Error: Ошибки модуля доступа к серверной базе данных. Система OLE DB сообщила о переполнении типа данных для столбца "dbo_vw_DimDeliveryDetailDeliveryExecutionDays3PL0_0".

> dimensions:Error: Ошибки модуля хранения OLAP: Ошибка при обработке атрибута "Delivery Execution Days 3PL" измерения "Delivery Detail" из базы данных "Marketplace".

Для сужения поиска, лучше запустить процессинг измерения из скрипта

```xml
<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
  <Process xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ddl2="http://schemas.microsoft.com/analysisservices/2003/engine/2" xmlns:ddl2_2="http://schemas.microsoft.com/analysisservices/2003/engine/2/2" xmlns:ddl100_100="http://schemas.microsoft.com/analysisservices/2008/engine/100/100" xmlns:ddl200="http://schemas.microsoft.com/analysisservices/2010/engine/200" xmlns:ddl200_200="http://schemas.microsoft.com/analysisservices/2010/engine/200/200" xmlns:ddl300="http://schemas.microsoft.com/analysisservices/2011/engine/300" xmlns:ddl300_300="http://schemas.microsoft.com/analysisservices/2011/engine/300/300" xmlns:ddl400="http://schemas.microsoft.com/analysisservices/2012/engine/400" xmlns:ddl400_400="http://schemas.microsoft.com/analysisservices/2012/engine/400/400" xmlns:ddl500="http://schemas.microsoft.com/analysisservices/2013/engine/500" xmlns:ddl500_500="http://schemas.microsoft.com/analysisservices/2013/engine/500/500">
    <Object>
      <DatabaseID>TEST_Marketplace</DatabaseID>
      <DimensionID>Delivery Detail</DimensionID>
    </Object>
    <Type>ProcessFull</Type>
    <WriteBackTableCreation>UseExisting</WriteBackTableCreation>
  </Process>
</Batch>
```

И скопировать вывод в [текстовый файл](./Error_Dims.log), в котором произвести поиск.

Параллельно открыть проект куба в VS, который лучше сгенерить по кубу. 

По файлу с выводом запущенных запросов сделать поиск столбца из ошибки. И сопоставить его со свойствами атрибута (Key, Value, Name). Находим два фрагмента

```
Begin SQL statement: SELECT 
		DISTINCT
	[dbo_vw_DimDeliveryDetail].[DeliveryExecutionDays3PL] AS [dbo_vw_DimDeliveryDetailDeliveryExecutionDays3PL0_0],[dbo_vw_DimDeliveryDetail].[deliveryexecutiondays3plname] AS [dbo_vw_DimDeliveryDetailDeliveryExecutionDays3PLName0_1]
  FROM [olap].[vw_DimDeliveryDetail] AS [dbo_vw_DimDeliveryDetail]
```

```
End SQL statement: SELECT 
		DISTINCT
	[dbo_vw_DimDeliveryDetail].[DeliveryExecutionDays3PL] AS [dbo_vw_DimDeliveryDetailDeliveryExecutionDays3PL0_0],[dbo_vw_DimDeliveryDetail].[deliveryexecutiondays3plname] AS [dbo_vw_DimDeliveryDetailDeliveryExecutionDays3PLName0_1]
  FROM [olap].[vw_DimDeliveryDetail] AS [dbo_vw_DimDeliveryDetail]
```

Видим, что "Система OLE DB" ругается на поле [olap].[vw_DimDeliveryDetail].[DeliveryExecutionDays3PL]

Смотрим в проект, это поле используется для формирования свойства Key атрибута "Delivery Execution Days 3PL". Далее анализируем диапазон значений и размер поля [olap].[vw_DimDeliveryDetail].[DeliveryExecutionDays3PL]

```sql
  SELECT min([dbo_vw_DimDeliveryDetail].[DeliveryExecutionDays3PL])
  , max([dbo_vw_DimDeliveryDetail].[DeliveryExecutionDays3PL])
  FROM [olap].[vw_DimDeliveryDetail] AS [dbo_vw_DimDeliveryDetail]
```

После выяснения причины ошибки, думаем как её лучше решить. Если есть возможность её обработать на уровне вьюхи, на которой строится атрибут. То исправляем вьюху и пробуем отпроцессить снова. В противном случае правим проект куба и раскатываем на **PROD**.

