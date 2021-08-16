## [Заметки по SQL Server](../SQLServer_note.md)  

### Перенос объектов слоя RAW в отдельную БД  

#### Описание проблемы  
В существующем проекте DWH возникла необходимость вынести слои в отдельные БД для более оптимального обслуживания баз. Для переноса нужно сделать изменения как в самой БД на сервере, так и в проекте. Также нужно внести изменения в проект ETL - пакетов.

#### Варианты решения  
Для реализации этой задачи можно использовать язык BIML, который является надстройкой над проектами и позволяет абстрагироваться от работы с XML. Или опуститься до самих файлов проектов и работать с ними напрямую.

#### Реализация  
Будем работать с файлами проекта напрямую с помощью C#. На первом этапе внесем изменения в проекты БД и создадим скрипты миграции. На втором этапе внесем изменения в проект SSIS.  
Для начала создадим command-line приложение и выведем список объектов слоя RAW.  
В проекте БД все объекты схемы расположены в папке с названием схемы. Нас интересуют все схемы, которые начинаются на raw. Выведем список этих папок.  

```c#
class Program
{
	private static string sourceRoot = @"C:\Users\zhigalov\Desktop\GOODS\GitLab\DWH\Databases\MDWH\";
	static void Main(string[] args)
	{
		try
		{
			string[] folderNames = Directory.GetDirectories(sourceRoot, "raw*", SearchOption.TopDirectoryOnly);
			Console.WriteLine("The number of directories starting with raw is {0}.", folderNames.Length);
			foreach (string fln in folderNames)
			{
				Console.WriteLine(fln);
			}
		}
		catch (Exception e)
		{
			Console.WriteLine("The process failed: {0}", e.ToString());
		}
		Console.ReadLine();
	}
}
```

Теперь выведем все объекты, которые расположены в этих папках в виде дерева

```c#
class Program
{
	private static string sourceRoot = @"C:\Users\zhigalov\Desktop\GOODS\GitLab\DWH\Databases\MDWH\";
	static void Main(string[] args)
	{
		try
		{
			string[] folderNames = Directory.GetDirectories(sourceRoot, "raw*", SearchOption.TopDirectoryOnly);
			Console.WriteLine("The number of directories starting with raw is {0}.", folderNames.Length);
			int prefixLen = sourceRoot.Length;
			string schemaName;
			foreach (string fln in folderNames)
			{
				schemaName = fln.Substring(prefixLen);
				Console.WriteLine(String.Format("    {0}/", schemaName));
				ProcessDirectory(fln, 1);

			}
		}
		catch (Exception e)
		{
			Console.WriteLine("The process failed: {0}", e.ToString());
		}
		Console.ReadLine();
	}

	// Process all files in the directory passed in, recurse on any directories
	// that are found, and process the files they contain.
	public static void ProcessDirectory(string targetDirectory, int lvl)
	{
		lvl = lvl + 1;

		// Process the list of files found in the directory.
		string[] fileEntries = Directory.GetFiles(targetDirectory);
		foreach (string fileName in fileEntries)
			ProcessFile(fileName, lvl);

		// Recurse into subdirectories of this directory.
		string[] subdirectoryEntries = Directory.GetDirectories(targetDirectory);
		int prefixLen = targetDirectory.Length + 1;
		foreach (string subdirectory in subdirectoryEntries)
		{
			Console.WriteLine(String.Format(string.Concat(Enumerable.Repeat("    ", lvl)) + "{0}/", subdirectory.Substring(prefixLen)));
			ProcessDirectory(subdirectory, lvl);
		}
	}

	public static void ProcessFile(string path, int lvl)
	{
		FileInfo fi = new FileInfo(path);
		Console.WriteLine(string.Concat(Enumerable.Repeat("    ", lvl)) + "{0}", fi.Name);
	}
}
```

Видим, что на следующем уровне находятся папки с названиями типов объектов БД. Сгруппируем все эти папки по названию и получим список типов объектов, которые нужно перенести. Для этого допишем код выше [следующим образом](./source/listObjType.cs.md).

```C#
List<string> distinct = listObjType.Distinct().ToList();
Console.WriteLine("listObjType:");
foreach (string value in distinct)
{
	Console.WriteLine("    {0}", value);
}
Console.ReadLine();
```

Получим следующий список типов объектов БД

```c#
listObjType:
    Stored Procedures
    Tables
    Table
    Views
```

Обратим внимание, что объекты типа таблица имеют два названия папки: Tables и Table. Учтём это!

Далее перейдём собственно к внесению изменений в проектах БД. Внесём изменения в проекте **MDWH_RAW**

- создадим схемы БД (создание папок и перемещение sql файлов из папки Security).  
- создание таблиц БД (создание папок и файлов, перемещение sql файлов из соответствующих папок).  
- создание представлений БД (создание папок и файлов, перемещение sql файлов из соответствующих папок). Учесть обращение к объектам в других БД.  
- создание процедур БД (создание папок и файлов, перемещение sql файлов из соответствующих папок). Учесть обращение к объектам в других БД.  

Внесём изменения в проекте **MDWH**

- удалим процедуры БД  
- удалим представления БД  
- удалим таблицы БД  
- удалим схемы БД  




#### Полезные ссылки:  
- [Directory.GetDirectories Method](https://docs.microsoft.com/en-us/dotnet/api/system.io.directory.getdirectories?view=netframework-4.7.2&source=docs)  
- [String.Substring Method](https://docs.microsoft.com/en-us/dotnet/api/system.string.substring?view=netframework-4.7.2)  
- [DirectoryInfo Class](https://docs.microsoft.com/en-us/dotnet/api/system.io.directoryinfo?view=netframework-4.7.2)  
- [FileInfo Class](https://docs.microsoft.com/en-us/dotnet/api/system.io.fileinfo?view=netframework-4.7.2)  
- [XmlDocument Class](https://docs.microsoft.com/en-us/dotnet/api/system.xml.xmldocument?view=netframework-4.7.2)

