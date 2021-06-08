# API клиент на C# для выгрузки из DWH  
*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)   

## Разобьём задачу на: 

- Реализовать клиент как обычную command line программу
- Перенести код в пакет SSIS в задачу Script Task
- Добавить логирование ошибок C# скрипта в пакете SSIS

### Реализовать клиент как обычную command line программу  

Перед разработкой скрипта в задаче Script Task в пакете SSIS, для отладки и простоты работы в VS создадим проект как обычную command line программу.  

Для подключения к SQL Server используем библиотеку System.Data.OleDb. Запрос к DWH лучше оформить во вьюху или функцию, чтобы в дальнейшем вносить изменения не меняя пакета SSIS.  Добавление ордера в таблицу логов оформить в виде процедуры. Выделим методы выгрузки ордеров в отдельный класс Upload. Получим в итоге классы [Program.cs](./UploadToAdjust/ConsoleApp/Program.cs.md) и [Upload.cs](./UploadToAdjust/ConsoleApp/Upload.cs.md)  

### Добавить логирование ошибок C# скрипта в пакете SSIS  

Отдельно обработаем исключение **WebException**

```c#
catch (WebException exception)
{
    string msg;
    using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
    {
        msg = String.Format("An error occurred in Script Task - Upload to Adjust: {0}", exception.Message.ToString());
        msg = msg + String.Format("\r\nException.Status is {0}", exception.Status.ToString());
        msg = msg + String.Format("\r\nWas transferred {0} orders", cnt.ToString());
        sw.Write(msg);
    }

    if (exception.Status == WebExceptionStatus.ProtocolError)
    {
        WebResponse resp = exception.Response;
        using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
        {
            using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
            {
                msg = msg + String.Format("\r\n{0}", sr.ReadToEnd());
                sw.WriteLine(sr.ReadToEnd());
            }
        }
    }

    msg = msg + String.Format("\r\nPath LogFolder is {0}", LogFolder);
    Dts.Events.FireError(0, "Script Task - Upload to Adjust", msg, "", 0);
    Dts.TaskResult = (int)ScriptResults.Failure;
}
```

### Исходный код

Uload to Adjust: [ScriptMain.cs](./UploadToAdjust/SSIS/ScriptMain.cs.md), [Upload.cs](./UploadToAdjust/SSIS/Upload.cs.md)  

### Полезные ссылки:  

- [Документация по библиотеке System.Data.OleDb](https://docs.microsoft.com/ru-ru/dotnet/api/system.data.oledb?view=netframework-4.6)  
- [Документация по классу Dictionary](https://docs.microsoft.com/ru-ru/dotnet/api/system.collections.generic.dictionary-2?view=netframework-4.5)  
- [Строки настраиваемых форматов даты и времени](https://docs.microsoft.com/ru-ru/dotnet/standard/base-types/custom-date-and-time-format-strings)  
- [Документация по Adjust на русском](https://docs.adjust.com/ru/)  
- [Документация по Adjust](https://help.adjust.com/en/article/server-to-server-events)  
- [Документация по Adjust - Encoding](https://help.adjust.com/en/article/encoding)  
- [Документация по Adjust - Placeholders](https://partners.adjust.com/placeholders/)  
- [JSON parser](https://jsonformatter.org/json-parser)  
- [Документация по классу WebException](https://docs.microsoft.com/ru-ru/dotnet/api/system.net.webexception?view=netframework-4.5)  
- [Документация по методу Thread.Sleep](https://docs.microsoft.com/ru-ru/dotnet/api/system.threading.thread.sleep?view=netframework-4.5)  