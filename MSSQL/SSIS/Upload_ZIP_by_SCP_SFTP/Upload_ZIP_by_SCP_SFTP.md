# Задача выгрузить данные из DWH партнёру  

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

**Разобьём задачу на:**  

-  Сформировать TXT-файлы выгрузки из DWH
-  Создать общий файл-архив выгрузки в формате ZIP
-  Передать файл-архив по SFTP партнёру

##  Сформировать CSV-файлы выгрузки из DWH  

Перед разработкой скрипта в задаче Script Task в пакете SSIS, для отладки и простоты работы в VS создадим проект как обычную command line программу.  

Для подключения к SQL Server используем библиотеку System.Data.OleDb. Выделим метод экспорта данных в отдельный класс. Получим в итоге класс [Program.cs](./Program.cs.md) и [Export.cs](./Export.cs.md).  

**Note**! Подход выгрузки данных через задачу Script Task **выигрывает тем, что** при изменении процедуры или вьюхи на стороне DWH не нужно вносить изменения в пакете. **Новый набор полей появится в файле автоматически!**  

После отладки кода в консольном проекте, перенесем его в  задание Script Task в пакете SSIS.




### Полезные ссылки:  
[Документация по библиотеке System.Data.OleDb](https://docs.microsoft.com/ru-ru/dotnet/api/system.data.oledb?view=netframework-4.6)  
[How to Export Data from SQL Server to Text File in C Sharp](http://www.techbrothersit.com/2016/04/c-how-to-export-data-from-sql-server.html)  
[List of escape characters for C#](./escape_characters.md)  