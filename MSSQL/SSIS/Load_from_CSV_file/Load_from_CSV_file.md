# Загрузка данных из CSV-файлов

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

## Решение

Организуем загрузку данных с помощью C#  скрипта. Для этого напишем обычное консольное приложение.
Для отладки в консоли (**cmd.exe**) при работе с файлами в разных кодировках нужно установить кодировку **UTF-8** ([список кодировок](https://docs.microsoft.com/ru-ru/dotnet/api/system.text.encoding?view=netframework-4.5))
```bash
chcp 65001
```
и выбрать шрифт **Lucida Console**. В коде для класса Console нужно установить свойство кодировки вывода

```c#
Console.OutputEncoding = Encoding.GetEncoding("UTF-8");
```

И последнее, правильно определить кодировку файла

```c#
File.ReadAllLines(file_path, Encoding.GetEncoding("Windows-1251"))
```

После переносим скрипт в пакет SSIS

### Исходный код

Exract from CSV-files: [main.cs](./main.cs.md) 

### Полезные ссылки:  
- [Элементы языка регулярных выражений](https://docs.microsoft.com/ru-ru/dotnet/standard/base-types/regular-expression-language-quick-reference)  
- [Регулярные выражения .NET](https://docs.microsoft.com/ru-ru/dotnet/standard/base-types/regular-expressions)  
- [Regex Класс](https://docs.microsoft.com/ru-ru/dotnet/api/system.text.regularexpressions.regex?view=netframework-4.5)  
- [Курс по регулярным выражениям](https://regexone.com/)  
- [Регулярные выражения *Фридл Дж* 2018.pdf](../../../RegEx/Регулярные выражения__Фридл Дж__2018.pdf)  
- [Encoding Класс](https://docs.microsoft.com/ru-ru/dotnet/api/system.text.encoding?view=netframework-4.5)  
- [Console Класс](https://docs.microsoft.com/ru-ru/dotnet/api/system.console?view=netframework-4.5)  
- [Set C# console application to Unicode output](https://stackoverflow.com/questions/38533903/set-c-sharp-console-application-to-unicode-output)  
- [Language Integrated Query (LINQ)](https://ru.wikipedia.org/wiki/Language_Integrated_Query)  
- [String.Split Метод](https://docs.microsoft.com/ru-ru/dotnet/api/system.string.split?view=netframework-4.5#System_String_Split_System_Char___System_StringSplitOptions_)  
- [Remove spaces inside the string](http://net-informations.com/q/faq/remove.html)  