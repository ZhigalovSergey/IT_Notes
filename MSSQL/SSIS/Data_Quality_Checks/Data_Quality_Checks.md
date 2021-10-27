### [Заметки по SSIS](../SSIS_note.md)  

## Проверки данных при загрузке в DWH из источников  

### Описание проблемы  
Часто бывает, что на стороне источника нет полей с датами создания и изменения записи. Есть либо запись с датой последнего изменения или вообще нет даты (например если таблица является справочником). В зависимости от того какие даты у нас есть и каким образом мы определяем данные для загрузки в хранилище (может быть использован подход с версионированием или загрузка по номеру транзакции, есть вариант использовать служебные таблицы с реестрами измененных строк).

### Рассмотрим следующие checks и возможные варианты загрузки и реализации  

#### Checks: duplicate key, null key, deleted row   

**Полная загрузка (для csv файла)**  

- **duplicate key**

  ```sql
    -- Checks new rows
    select
    	  [file_name]
    	  ,[order_queue_id]
    	  ,[tran_date]
    	  ,[accural_type]
    	  ,[fee_amount]
    	  ,[discount_amount]
    	  ,[sign_discount_amount]
    	  ,count(*) over (partition by [file_name], [order_queue_id], [sign_discount_amount] order by (select null)) cnt
    into #checks
    from
    	MDWH_STG.stg_bank_reestr_sberspasibo.[transaction]
    
    -- Check duplicate key
    insert into MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error (
    	   [file_name]
    	  ,[row]
    	  ,[description]
    )
    select
    	   [file_name]
    	  ,isnull(cast([order_queue_id] as nvarchar),'')+';'+isnull(cast([tran_date] as nvarchar),'')+';'+isnull([accural_type],'')+';'+isnull(cast([fee_amount] as nvarchar),'')+';'+isnull(cast([discount_amount] as nvarchar),'') as [row]
    	  ,'insert duplicate key'
    from
    	#checks
    where
    	cnt > 1
  ```

- **null key**

  ```sql
    -- Check null key
    insert into MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error (
    	   [file_name]
    	  ,[row]
    	  ,[description]
    )
    select
    	   [file_name]
    	  ,isnull(cast([order_queue_id] as nvarchar),'')+';'+isnull(cast([tran_date] as nvarchar),'')+';'+isnull([accural_type],'')+';'+isnull(cast([fee_amount] as nvarchar),'')+';'+isnull(cast([discount_amount] as nvarchar),'') as [row]
    	  ,'insert null key'
    from
    	#checks
    where
    	cnt = 1
    	and [order_queue_id] is null
  ```

- **deleted row** (catch the change)

 **Загрузка изменений (новые записи + измененные записи)**  



### Исходный код скриптов

- [Полная загрузка (для csv файла)](./transaction_sync.sql.md)  
- [Рассылка ошибок](./transaction_error_send_email.sql.md)  

