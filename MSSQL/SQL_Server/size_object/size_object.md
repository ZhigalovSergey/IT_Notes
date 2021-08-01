# Узнать размер таблицы

Чтобы посмотреть сколько занимает места таблица используем sp_spaceused

```sql
-- Для таблицы core.items
exec sp_spaceused @objname = N'core.items', @updateusage = N'TRUE';

-- Для текущей БД
exec sp_spaceused @updateusage = N'TRUE';
```

Узнать размеры индексов в таблице можно с помощью запроса

```sql
SELECT
    i.name                  AS IndexName,
    SUM(s.used_page_count) * 8   AS IndexSizeKB
FROM sys.dm_db_partition_stats  AS s 
JOIN sys.indexes                AS i
ON s.[object_id] = i.[object_id] AND s.index_id = i.index_id
WHERE s.[object_id] = object_id('core.item_snapshot')
GROUP BY i.name
ORDER BY i.name
```

Свободное место на дисках сервера

```sql
EXEC master.sys.xp_fixeddrives
```



### Полезные ссылки:  

- [DBCC SHRINKDATABASE](https://docs.microsoft.com/ru-ru/sql/t-sql/database-console-commands/dbcc-shrinkdatabase-transact-sql?view=sql-server-ver15)  
- [DBCC SHRINKFILE](https://docs.microsoft.com/ru-ru/sql/t-sql/database-console-commands/dbcc-shrinkfile-transact-sql?view=sql-server-ver15)  
- [sp_spaceused](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-spaceused-transact-sql?view=sql-server-ver15)  
- [Почему вы не должны сжимать ваши файлы данных](https://habr.com/ru/post/330492/)  

