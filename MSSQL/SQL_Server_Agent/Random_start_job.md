#### [Заметки по SQL Server Agent](./SQLAgent_note.md)  

### Случайный запуск задания (Job)

#### Описание проблемы

Иногда Jobs по расписанию должны запускаться в один момент. В результате, служба некоторые запуски не отрабатывает и выдается ошибка "The operation failed because the execution timed out."

>  Message
> Executed as user: CORP\MSAmir-sdb-005$. Microsoft (R) SQL Server Execute Package Utility  Version 14.0.3048.4 for 64-bit  Copyright (C) 2017 Microsoft. All rights reserved.    Started:  9:30:29  Failed to execute IS server package because of error 0x80131904. Server: mir-sdb-005, Package path: \SSISDB\Input\Input\oms_measurement_protocol.dtsx, Environment reference Id: 9.  Description: The operation failed because the execution timed out.  Source: .Net SqlClient Data Provider  Started:  9:30:29  Finished: 9:30:43  Elapsed:  13.829 seconds.  The package execution failed.  

Для устранения этой ошибки нужно разнести запуски заданий по времени.

#### Решение  

- Можно посмотреть расписания запусков и руками их разносить.

- Другой подход, добавить первым шагом в каждый Job код с рендомной задержкой.

  ```sql
  declare @MaxDelay int = 180
  declare @Delay int = 0
  declare @DelayLength datetime = convert(datetime, 0)
  
  set @Delay = floor(rand()*@MaxDelay)
  set @DelayLength = dateadd(second, @Delay%60, dateadd(minute, @Delay/60, @DelayLength))
  
  waitfor delay @DelayLength
  ```
  
  
  


#### Полезные ссылки:  

- [Generate Unique Random Number in SQL Server](https://www.mssqltips.com/sqlservertip/6313/generate-unique-random-number-in-sql-server/)  
