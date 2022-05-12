### [ Заметки по Python](../python.md)
## Унификация выгрузки данных из хранилища в GBQ  
### Описание проблемы  
Выгрузки в GBQ однотипны и на первый взгляд поддаются шаблонизации. Для примера можно взять скрипт выгрузки [upload_promotion_to_gbq](./upload_promotion_to_gbq.py). Скрипт для первоначальной загрузки [upload_promotion_to_gbq_load](./upload_promotion_to_gbq_load.py).  
### Варианты решения    
Попробуем передавать всю необходимую информацию через JSON файл

### Реализация  

На основе файла скрипта видно, что  JSON файл должен содержать информацию о таблице и отдельные участки кода: запрос для извлечения данных, запрос для вставки и обновления данных.

В итоге файл для примера выше выглядит следующим образом

```json
{
"dwh_table" : "raw_mms_mss_merchantpromo.promotion", 
"gbq_table" : "dwh_input.promotion", 
"gbq_table_tmp" : "dwh_input.promotion_tmp", 
"gbq_columns" :
			[
			{"name": "promotion_id", "type": "INTEGER"}, 
			{"name": "name", "type": "STRING"},
			{"name": "type_id", "type": "INTEGER"}, 
			{"name": "merchant_id", "type": "INTEGER"}, 
			{"name": "mt_update_dt", "type": "DATETIME"}
			],
"sql_get" : "./get_promotion.sql",
"sql_insert" : "./insert_promotion.sql",
"sql_update" : "./update_promotion.sql"
}
```

Для генерирования скрипта используем консольную программу на C#

