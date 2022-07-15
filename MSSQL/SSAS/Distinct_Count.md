## Подсчёт меры distinct count в кубе

### Описание проблемы

Для подсчёта меры distinct count создается отдельная группа мер, что ведет к увеличению времени процессинга куба.

### Варианты решения 

Для оптимизации процессинга постараемся изменить модель хранилища и куба так, чтобы подсчёт не аддитивной меры distinct count стал аддитивным. Другой вариант, написать вычисляемую меру через scope.

### Реализация

Через изменение структуры хранилища и куба. Допутим у нас есть таблица Lots, в которой есть идентификатор покупателя Сustomer. И мы хотим уйти от подсчёта уникальных покупателей за единицу времени.

![](.\DC.jpg)

Реализация Distinct Count через calculations

```sql
SCOPE ([Measures].[DC Customer Key], [Customer].[Customer].[Customer]);         
  This = 1;         
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE;    

SCOPE ([Measures].[DC Order ID], [Order Detail].[Order ID].[Order ID]);         
  This = 1;          
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE;

SCOPE ([Measures].[DC Delivery ID], [Delivery Detail].[Delivery ID].[Delivery ID]);         
  This = 1;          
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE; 

SCOPE ([Measures].[DC Shipment Order ID], [Shipment Order Detail].[Shipment Order ID].[Shipment Order ID]);         
  This = 1;         
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE;

SCOPE ([Measures].[DC Product Item Key], [Product].[Article Number].[Article Number]);         
  This = 1;          
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE;    

SCOPE ([Measures].[DC Ubc Key], [Ubc].[Ubc Key].[Ubc Key]);         
  This = 1;          
  non_empty_behavior(This) = [Measures].[Ordered Quantity];         
END SCOPE;   

SCOPE ([Measures].[DC Loyalty Lot], [Lot Detail].[Lot ID].[Lot ID]);         
  This = 1;          
  non_empty_behavior(This) = [Measures].[Loyalty Points Value];         
END SCOPE;  
```

```sql
CREATE MEMBER CURRENTCUBE.[Measures].[Buyer Count]
 AS [Measures].[DC Customer Key], 
FORMAT_STRING = "#,#", 
VISIBLE = 1 ,  ASSOCIATED_MEASURE_GROUP = 'Orders';                     
                                                                             
CREATE MEMBER CURRENTCUBE.[Measures].[Delivery Count]
 AS [Measures].[DC Delivery ID], 
FORMAT_STRING = "#,#", 
VISIBLE = 1 ,  ASSOCIATED_MEASURE_GROUP = 'Orders';                                                    
CREATE MEMBER CURRENTCUBE.[Measures].[Shipment Order Count]
 AS [Measures].[DC Shipment Order ID], 
FORMAT_STRING = "#,#", 
VISIBLE = 1 ,  ASSOCIATED_MEASURE_GROUP = 'Orders';                                                                                               
CREATE MEMBER CURRENTCUBE.[Measures].[SKU Count]
 AS [Measures].[DC Product Item Key], 
FORMAT_STRING = "#,#", 
VISIBLE = 1 ,  ASSOCIATED_MEASURE_GROUP = 'Orders';   
```



### Полезные ссылки

- [Different options for creating a distinct count measure in SSAS](https://www.mssqltips.com/sqlservertip/3043/different-options-for-creating-a-distinct-count-measure-in-ssas/)  