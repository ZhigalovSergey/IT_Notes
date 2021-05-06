# Задача выгрузить данные из DWH партнёру  

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

## **Разобьём задачу на:**  

-  Сформировать TXT-файлы выгрузки из DWH
-  Создать общий файл-архив выгрузки в формате ZIP
-  Передать файл-архив по SFTP партнёру

###  Сформировать CSV-файлы выгрузки из DWH  

Перед разработкой скрипта в задаче Script Task в пакете SSIS, для отладки и простоты работы в VS создадим проект как обычную command line программу.  

Для подключения к SQL Server используем библиотеку System.Data.OleDb. Выделим метод экспорта данных в отдельный класс. Получим в итоге класс [Program.cs](./Program.cs.md) и [Export.cs](./Export.cs.md).  

**Note**! Подход выгрузки данных через задачу Script Task **выигрывает тем, что** при изменении процедуры или вьюхи на стороне DWH не нужно вносить изменения в пакете. **Новый набор полей появится в файле автоматически!**  

После отладки кода в консольном проекте, перенесем его в  задание Script Task в пакете SSIS.

```c#
DateTime thisDay = DateTime.Today;
DateTime SunDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek);
DateTime MonDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek - 6);
string date_from = MonDay.ToString("yyyyMMdd");
string date_to = SunDay.ToString("yyyyMMdd");

//Declare Variables and provide values
string FileNamePart = "GOODS_ADDRESS" + "_" + date_from + "_" + date_to;  //Datetime will be added to it
string DestinationFolder = @"C:\Users\zhigalov\Desktop\GOODS\TASKS\4228\upload_to_gfk\";
string FileDelimiter = "\t";        //You can provide comma or pipe or whatever you like
string FileExtension = ".txt";      //Provide the extension you like such as .txt or .csv

//Create Connection to SQL Server in which you like to load files
string MDWHConnection = "Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False";
string queryString = String.Format("exec interface.gfk_cities");

//Read data from SQL SERVER
Export ex = new Export();
ex.Export_to_flat_file(MDWHConnection, queryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

```

### Создать общий файл-архив выгрузки в формате ZIP  

Для создания ZIP-архива используем внешнюю программу 7z.exe. При этом нужно создавать временную папку для файлов созданных на предыдущем этапе, а после формирования архива эту папку удалять. Поэтому реализовывать эти действия лучше в задаче **Script Task**, созданной на предыдущем этапе, чем создавать несколько **Execute Process Task**.

```c#
public void CreateZip(string SourceName, string TargetName)
{
    ProcessStartInfo p = new ProcessStartInfo
    {
        FileName = Dts.Variables["ZipExecutable"].Value.ToString(),
        Arguments = "a -t7z \"" + TargetName + "\" \"" + SourceName + "\"",
        WindowStyle = ProcessWindowStyle.Hidden
    };
    Process x = Process.Start(p);
    x.WaitForExit();
}


public void Main()
{

    DateTime thisDay = DateTime.Today;
    DateTime SunDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek);
    DateTime MonDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek - 6);
    string date_from = MonDay.ToString("yyyyMMdd");
    string date_to = SunDay.ToString("yyyyMMdd");

    //Declare Variables and provide values
    string FileNamePart = "GOODS_ADDRESS" + "_" + date_from + "_" + date_to;  //Datetime will be added to it
    string DestinationFolder = String.Format("{0}/temp/", Dts.Variables["WorkingDirectory"].Value);
    string FileDelimiter = "\t";        //You can provide comma or pipe or whatever you like
    string FileExtension = ".txt";      //Provide the extension you like such as .txt or .csv

    //Create Connection to SQL Server in which you like to load files
    string MDWHConnection = "Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False";
    string queryString = String.Format("exec interface.gfk_cities");

    //Create DestinationFolder
    if (!Directory.Exists(DestinationFolder))
    {
        Directory.CreateDirectory(DestinationFolder);
    }

    //Read data from SQL SERVER
    Export ex = new Export();
    ex.Export_to_flat_file(MDWHConnection, queryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

    FileNamePart = String.Format("GOODS_MAPPING_{0}_{1}", date_from, date_to);
    queryString = String.Format("exec interface.gfk_weekly_assortment {0}, {1}", date_from, date_to);
    ex.Export_to_flat_file(MDWHConnection, queryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

    FileNamePart = String.Format("GOODS_{0}_{1}", date_from, date_to);
    queryString = String.Format("exec interface.gfk_orders {0}, {1}", date_from, date_to);
    ex.Export_to_flat_file(MDWHConnection, queryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

    //Create zip file
    string SourceName = String.Format("{0}/temp/*.*", Dts.Variables["WorkingDirectory"].Value);
    string TargetName = String.Format("{0}/upload_to_gfk/GOODS_{1}_{2}.zip", Dts.Variables["WorkingDirectory"].Value, date_from, date_to);
    CreateZip(SourceName, TargetName);

    //Delete DestinationFolder
    Directory.Delete(DestinationFolder, true);

    Dts.TaskResult = (int)ScriptResults.Success;
}
```

### Передать файл-архив по SFTP партнёру  

Перед разработкой скрипта в задаче Script Task в пакете SSIS, для отладки и простоты работы в VS создадим проект как обычную command line программу. Для подключения к удаленному серверу по SSH используем библиотеку [Renci.SshNet](https://github.com/sshnet/SSH.NET). При переносе кода в задание Script Task в пакете SSIS нужно добавить библиотеку в GAC. Для этого можно использовать следующий [bat-файл](./GAC_Reg.bat.md). Чтобы использовать Sensitive переменные нужно проект и все пакеты перевести на [Protection Level](https://www.mssqltips.com/sqlservertip/2091/securing-your-ssis-packages-using-package-protection-level/) = EncryptSensitiveWithPassword. А для извлечения свойства в задаче Script Task нужно использовать метод GetSensitiveValue(). Поменять свойства во всех пакетах можно с помощью расширения [BI Developer Extensions](https://bideveloperextensions.github.io/features/BatchPropertyUpdate/) , Property Path: **\Package.Properties[ProtectionLevel]**

После переноса получаем:

```c#
public void Main()
{
    string host = @"gimftp.gfk.com";
    string username = "GFK003953";
    string password = Dts.Variables["gfk_pass"].GetSensitiveValue().ToString();

    PrivateKeyFile keyFile = new PrivateKeyFile(String.Format("{0}/gfk/Nielsen_MFT_key/nielsen_goods_private_key_openssh.ppk", Dts.Variables["WorkingDirectory"].Value));
    var keyFiles = new[] { keyFile };

    var methods = new List<AuthenticationMethod>
    {
        new PasswordAuthenticationMethod(username, password),
        new PrivateKeyAuthenticationMethod(username, keyFiles)
    };

    Renci.SshNet.ConnectionInfo con = new Renci.SshNet.ConnectionInfo(host, 22, username, methods.ToArray());
    using (var client = new SftpClient(con))
    {
        client.Connect();

        string LocalPath = String.Format("{0}/gfk/upload_to_gfk/", Dts.Variables["WorkingDirectory"].Value);
        string RemotePath = @"./";

        string[] files = Directory.GetFiles(LocalPath, "*.zip", SearchOption.TopDirectoryOnly);

        foreach (string file in files)
        {
            string fileName = file.Substring(LocalPath.Length);
            var fileStream = new FileStream(file, FileMode.Open);
            if (fileStream != null)
            {
                client.UploadFile(fileStream, String.Format("{0}/{1}", RemotePath, fileName), null);
            }
        }

        client.Disconnect();
    }

    Dts.TaskResult = (int)ScriptResults.Success;
}
```

**После анализа кода приходим к выводу, что всю логику выгрузки данных удобнее реализовать в одном Script Task.**  

### Исходный код скриптав для выгрузки данных из DWH  

- Для GFK: [ScriptMain.cs](./GFK/ScriptMain.cs.md), [Export.cs](./GFK/Export.cs.md), [Upload.cs](./GFK/Upload.cs.md)
- Для Nielsen: [ScriptMain.cs](./Nielsen/ScriptMain.cs.md), [Export.cs](./Nielsen/Export.cs.md), [Upload.cs](./Nielsen/Upload.cs.md)

### Полезные ссылки:  
- [Документация по библиотеке System.Data.OleDb](https://docs.microsoft.com/ru-ru/dotnet/api/system.data.oledb?view=netframework-4.6)  
- [How to Export Data from SQL Server to Text File in C Sharp](http://www.techbrothersit.com/2016/04/c-how-to-export-data-from-sql-server.html)  
- [List of escape characters for C#](./escape_characters.md)  
- [Zip and Unzip files using 7-Zip](https://www.mssqltips.com/sqlservertip/5756/zip-and-unzip-files-using-7zip-in-sql-server-integration-services-ssis/)  
- [Renci.SshNet](https://github.com/sshnet/SSH.NET) 
- [How to access a SFTP server](https://ourcodeworld.com/articles/read/369/how-to-access-a-sftp-server-using-ssh-net-sync-and-async-with-c-in-winforms)  
- [winscp.exe - synchronize command](https://winscp.net/eng/docs/scriptcommand_synchronize)  
- [Документация по классу Directory](https://docs.microsoft.com/ru-ru/dotnet/api/system.io.directory?view=netframework-4.6)
- [C# list directory](https://zetcode.com/csharp/listdirectory/)  
- [Access Control for Sensitive Data in Packages](https://docs.microsoft.com/en-us/sql/integration-services/security/access-control-for-sensitive-data-in-packages?view=sql-server-2016&WT.mc_id=DP-MVP-5001430)  
- [Документация по классу TimeSpan](https://docs.microsoft.com/ru-ru/dotnet/api/system.timespan?view=netframework-4.6)  
- [Документация по классу DateTime](https://docs.microsoft.com/ru-ru/dotnet/api/system.datetime?view=netframework-4.6)  
- [Документация по классу CultureInfo](https://docs.microsoft.com/ru-ru/dotnet/api/system.globalization.cultureinfo.currentculture?view=netframework-4.6)  
- [Raising Events in the Script Task](https://docs.microsoft.com/en-us/sql/integration-services/extending-packages-scripting/task/raising-events-in-the-script-task?view=sql-server-ver15)  
- [Script Task and Component Logging](http://microsoft-ssis.blogspot.com/2011/02/script-task-and-component-logging.html)  