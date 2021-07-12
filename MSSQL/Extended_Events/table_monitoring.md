## Мониторинг таблицы

### Описание проблемы

Иногда появляется вопрос, а кто-нибудь пользуется вообще этой таблицей? Может не стоит грузить сервер для обновления данных в этой таблице.

### Варианты решения

Первое, что можно сделать - это настроить мониторинг запросов **SELECT** к этой таблице. Для этого используем Extended Events.

### Реализация

Настроить событие Extended Events можно двумя способами: с помощью пользовательского интерфейса SSMS и с помощью инструкции T-SQL. Обе реализации описаны в [документации](https://docs.microsoft.com/ru-ru/sql/relational-databases/extended-events/quick-start-extended-events-in-sql-server?view=sql-server-ver15). Так для отслеживанию таблицы product_sort_attributes можно использовать следующий шаблон

```sql
N'%select%product_sort_attributes%'
```

для поля sqlserver.sql_text в настройках фильтра.

### Полезные ссылки:  

- [Database alias in Microsoft SQL Server](https://www.baud.cz/blog/database-alias-in-microsoft-sql-server)  
- [mssqltips.com - SQL Server Extended Events Tutorial](https://www.mssqltips.com/sqlservertutorial/9194/sql-server-extended-events-tutorial/)  

