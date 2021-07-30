## Загрузка больших объемов данных

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

### Описание проблемы

Иногда загрузка исторических данных длится очень долго. В результате пакет падает или зависает.

### Варианты Решения

Задачу загрузки больших данных можно разбить на две

- Организация загрузки по частям
- Организация параллельной загрузки

### Реализация. Организация загрузки по частям

Для решения первой задачи нужно выбрать столбец по которому данные распределены равномерно. И организовать загрузку по этим значениям. Часто это бывает дата. В нашем случае - snapshot_date

Запрос для получения списка дат, результат которого помещаем в переменную типа Object

```sql
select cast(cast(prv.[value] as date) as nvarchar) dt
from sys.partition_functions as pf
	left join sys.partition_range_values as prv on
		prv.function_id = pf.function_id
where name = 'pf_item_snapshot' and cast(prv.[value] as date) > '2020-12-21'
order by cast(prv.[value] as date)
```

![](./ListDate.jpg)

Далее настроим контейнер ForEach 

![](./ForEach.jpg)

Определим переменную для итераций

![](./ForEach_2.jpg)

Так как данные могут грузиться днями, то нужно предусмотреть окна для работы сервера по обновлению данных. Сделаем это с помощью запроса

```sql
if datepart(hh, getdate()) < 9
	waitfor time '09:00:00'
```

В итоге получается такой пакет

![](./Package.jpg)

### Реализация. Организация параллельной загрузки

Для организации параллельной загрузки нужно разработать диспетчер, который будет записывать какой процесс какие данные грузит. В качестве такого диспетчера сделаем таблицу со следующей структурой

```sql
create table tempdb.dbo.map_of_tasks (
	id int not null identity (1, 1),
	ExecutionInstanceGUID nvarchar(128),
	task_id int,
	insert_dt datetime
)
```





### Реализация. Загрузка INSERT INTO…SELECT для массового импорта данных с минимальным ведением журнала и параллелизмом

> Для минимального протоколирования этой инструкции необходимо выполнение следующих требований.
>
> - Модель восстановления базы данных настроена на простое или неполное протоколирование.
> - Целевой таблицей является пустая или непустая куча.
> - Целевая таблица не используется в репликации.
> - Для целевой таблицы используется указание `TABLOCK`.



### Полезные ссылки:  

[Предыдущие выпуски SQL Server Data Tools (SSDT и SSDT-BI)](https://docs.microsoft.com/ru-ru/sql/ssdt/previous-releases-of-sql-server-data-tools-ssdt-and-ssdt-bi?view=sql-server-ver15#ssdt-for-visual-studio-vs-2017)  
[Заметки о выпуске SQL Server Management Studio (SSMS)](https://docs.microsoft.com/ru-ru/sql/ssms/release-notes-ssms?view=sql-server-ver15#previous-ssms-releases)  
[Moving the SSISDB Catalog on a new SQL Server instance](https://www.sqlshack.com/moving-the-ssisdb-catalog-on-a-new-sql-server-instance/)  
[SQL Server Table Partitioning: Resources](https://www.brentozar.com/sql/table-partitioning-resources/)  
[Инструкция INSERT](https://docs.microsoft.com/ru-ru/sql/t-sql/statements/insert-transact-sql?view=sql-server-ver15)  
