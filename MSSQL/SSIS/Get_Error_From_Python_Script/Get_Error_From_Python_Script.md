# Логирование ошибок Python скрипта в пакете SSIS  

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

## Описание проблемы  
При запуске Python скриптов в пакетах SSIS, ошибки возникающие при выполнении скрипта не передаются в отчёт по выполнению пакета. Поэтому приходится открывать пакет, находить скрипт и запускать его руками, чтобы увидеть ошибку.  

## Решение  
Запускать скрипт в задаче Script Task и использовать классы ProcessStartInfo для запуска скрипта, Process для анализа возникновения ошибки и её вывода.  

## Реализация  
Для того чтобы не запускалось окно выполнения скрипта, нужно использовать python**w**.exe. Для генерации события об ошибки и передачи сообщения в отчёт по выполнению пакета SSIS используем **Dts.Events.FireError**.  
В данном случае также используем переменные: User::file_py (полное имя файла-скрипта), User::login (логин для подключения к сервису), User::PythonExecutable (интерпретатор python C:\Python37\pythonw.exe) и параметр $Project::nadavi_pass (пароль для подключения к сервису)  
```c#
ProcessStartInfo p = new ProcessStartInfo
{
    UseShellExecute = false,
    RedirectStandardError = true,
    FileName = Dts.Variables["PythonExecutable"].Value.ToString(),
    Arguments = Dts.Variables["file_py"].Value.ToString() + " " + Dts.Variables["login"].Value.ToString() + " " + Dts.Variables["nadavi_pass"].GetSensitiveValue().ToString(),
    WindowStyle = ProcessWindowStyle.Hidden
};
Process x = Process.Start(p);
x.WaitForExit();
if (x.ExitCode != 0)
{
    StreamReader myStreamReader = x.StandardError;
    Dts.Events.FireError(0, "Script Task", "An error occurred in Script Task: " + myStreamReader.ReadToEnd(), "", 0);
}
```

### Исходный код  
[advertising_cost.cs](./ScriptMain/advertising_cost.cs.md)  
[measurement_protocol.cs](./ScriptMain/measurement_protocol.cs.md)  

### Полезные ссылки:  
- [Документация по классу ProcessStartInfo](https://docs.microsoft.com/ru-ru/dotnet/api/system.diagnostics.processstartinfo?view=netframework-4.5)  
- [Документация по классу Process](https://docs.microsoft.com/ru-ru/dotnet/api/system.diagnostics.process?view=netframework-4.5)  
- [Script Task and Component Logging](http://microsoft-ssis.blogspot.com/2011/02/script-task-and-component-logging.html)  