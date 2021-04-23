# Как написать REST клиент на C# для пакета SSIS

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SSIS](../SSIS_note.md)  

## Инструкция  
### Написать клиент как обычную Command Line программу.  
**Note**! В пакетах SSIS по умолчанию используется последняя мажорная версия. На текущи момент 7.0. Для использования [ключевых слов async и await](https://docs.microsoft.com/ru-ru/dotnet/csharp/programming-guide/concepts/async/) необходима версия 7.1 или выше.  

- #### Создаем консольное приложение в VS на основе классов **WebRequest** и **WebResponse** библиотеки **System.Net**.  

    - Первым делом нужно подключиться к сервису.  
    
      Ниже часть кода клиента [cloudpayment](./cloudpayment.cs.md). Об API сервиса можно почитать [здесь](https://developers.cloudpayments.ru/#api).  
    
    ```c#
    WebRequest request = WebRequest.Create(url);
    request.Method = "POST";
    request.ContentType = "application/json";
    request.Credentials = credentialCache;
    request.ContentLength = postBytes.Length;
    request.GetRequestStream().Write(postBytes, 0, postBytes.Length);
    
    // Get the response.
    WebResponse response = request.GetResponse();
    Stream dataStream = response.GetResponseStream();
    StreamReader reader = new StreamReader(dataStream, Encoding.UTF8);
    string result = reader.ReadToEnd();
    response.Close();
    ```
    
    - Далее нужно распарсить полученный JSON и преобразовать его в объекты .NET. Для этого нужно установить библиотеку [Newtonsoft](https://www.newtonsoft.com/json/help/html/Introduction.htm). А с помощью программ [парсинга JSON файлов](https://jsonformatter.org/json-parser) легко опрелелить какие куски JSON в какие объекты .NET лучше преобразовать. 
    
    ```c#
    //Для контроля работы клиента
    //Console.WriteLine(result);
    //Для парсинга в сторонней программе
    //File.WriteAllText("result.json", result);
    
    JObject resultSearch = JObject.Parse(result);
    IList<JToken> Jpayments = resultSearch["Model"].Children().ToList();
    
    // serialize JSON results into .NET objects
    IList<Payment> payments = new List<Payment>();
    foreach (JToken Jpayment in Jpayments)
    {
        // JToken.ToObject is a helper method that uses JsonSerializer internally
        Payment payment = Jpayment.ToObject<Payment>();
        payments.Add(payment);
    }
    ```
    
    Программы [конвертации JSON в класс C#](https://json2csharp.com/) помогают сформировать структуру объекта.  
    
    ```c#
    [JsonObject]
    public class Payment
    {
        [JsonProperty("PublicId")]
        public string PublicId { get; set; }
    
        [JsonProperty("TerminalUrl")]
        public string TerminalUrl { get; set; }
    
        [JsonProperty("TransactionId")]
        public string TransactionId { get; set; }
    
        [JsonProperty("Amount")]
        public string Amount { get; set; }
    
        [JsonProperty("Currency")]
        public string Currency { get; set; }
    ```

### Далее переходим к написанию клиента в пакете SSIS  
Первым делом нужно убедиться, что в [GAC](https://docs.microsoft.com/ru-ru/dotnet/framework/app-domains/gac) (Global Assembly Cache) зарегистрирована библиотека **NewtonSoft**, которую мы установили на предыдущем шаге. Для этого используем программу gacutil.exe  

Далее есть два варианта добавить API клиента в пакет SSIS: в поток Control Flow (Script Tasks) и в поток Data Flow (Script Components).

- #### Добавим API клиент в поток Data Flow.

  **Рекомендация**. Загрузать данные **на сторону DWH без конвертации на стороне клиента или пакета SSIS**, если данных до 1 Gb и конвертация не приведет к значительному уменьшению времени загрузки. Т.к. дальнейшую поддержку конвертации (исправление ошибок, дополнительная настройка условий) проще и быстрее осуществить на стороне  DWH (исправив процедуру), чем вносить изменение в пакет (загрузжать проект, открывать VS для исправления ошибок, делать code review XML файла пакета, меджить файл проекта и тд).
  
  **Рекомендация**. При написании клиента стоит позаботиться о слабом месте реляционных баз данных - предопределенная структура. И использовать предопределённое фиксированное количество столбцов и нейминг, а не генерировать запросы на вставку (insert into table (column_list) values (value_list)) исхдя из содержимого JSON файла.
  
  **Для размышления.** Если нет нужды трансформировать данные в потоке Data Flow (Script Components) после получения и количество полей набора данных велико, то вщзможно стоит реализовать клиент в Control Flow (Script Tasks). Т.к. перечислять все выходные поля в компоненте Script Components долго. Но вставка данных в клиенте будет посточная, в то время как в потоке Data Flow в компоненте OLE DB Destination можно использовать Fast Load.
  
  **Если количество полей набора данных велико**. Для частичной автоматизации процесса разработки клиента можно использовать:
  
  - для написания структуры класса [Конвертор JSON to C#](https://json2csharp.com/). В нашем случае класс для объекта большой и его лучше вынести в [отдельный файл](./cloudpayments_payment.cs.md).  
  
  - далее по сформированной структуре можно сгенерировать список полей, используя класс System.Reflection для [BIML скрипта компонента](https://docs.varigence.com/biml/snippets/script-projects-script-component-project) и файла main.cs. В итоге получаем такие файлы: [для BIML скрипта](./Payment_for_biml.txt.md) и [C# класса](./Payment_for_cs.txt.md)  
  
    ```c#
    File.WriteAllText("Payment_for_cs.txt", String.Empty);
    File.WriteAllText("Payment_for_biml.txt", String.Empty);
    
    PropertyInfo[] props = typeof(Payment).GetProperties();
    Console.WriteLine("Properties of Payment's class are:");
    foreach (PropertyInfo prop in props)
    {
        Console.WriteLine(prop.PropertyType +" | " + prop.Name);
        File.AppendAllText("Payment_for_cs.txt", "PaymentsBuffer." + prop.Name + " = " + "p." + prop.Name + ";" + Environment.NewLine);
        File.AppendAllText("Payment_for_biml.txt", "<Column Name = \"" + prop.Name + "\" DataType = \"String\" Length = \"4000\"/>" + Environment.NewLine);
    
    }
    Console.ReadLine();
    ```  
  
  - для написания скрипта BIML и последующей генераци пакета можно использовать надстройку [BimlExpress](https://varigence.com/BimlExpress) и пример для [генерации компонента](https://docs.varigence.com/biml/snippets/script-projects-script-component-project). Код для  [cloudpayments.biml](./cloudpayments.biml.md).  
  
  Для клиента cloudpayment добавим параметры для паролей (**cloudpaymentspass**) и загрузку за последние дни (**lagday**). Основной метод разработанный на предыдущем шаге остается без изменений.  
  
  ```c#
  List<Cred> logins = new List<Cred>
  {
      new Cred()
      {
          login = "pk_d9bd6dfb3b5d2324fe90180b2ee67",
          pass = Variables.cloudpaymentspass1
      },
      new Cred()
      {
          login = "pk_6275b61397528bfd45f6b93760942",
          pass = Variables.cloudpaymentspass2
      },
      new Cred()
      {
          login = "pk_e15776adc4a8465434a82e7ff284e",
          pass = Variables.cloudpaymentspass3
      },
      new Cred()
      {
          login = "pk_caedae6431186e7522c8959bce68d",
          pass = Variables.cloudpaymentspass4
      },
      new Cred()
      {
          login = "pk_fdc98bccae4edc64043e07d4869b9",
          pass = Variables.cloudpaymentspass5
      }
  };
  
  DateTime thisDay = DateTime.Today;
  DateTime lagDay = thisDay.AddDays(Variables.lagday);
  var payments = new List<Payment>();
  while (thisDay.Date >= lagDay.Date)
  {
      foreach (Cred l in logins)
      {
          payments.AddRange(GetData(l.login, l.pass, lagDay.ToString("yyyy-MM-dd")));
      }
  
      lagDay = lagDay.AddDays(1);
  }
  
  ListPayments = payments;
  ```  

## Поздравляю, клиент готов!  

### Примеры клиентов API на C# в пакете SSIS:  
[nadavi.net](./nadavi.cs.md)  
[cloudpayments.ru](./cloudpayments_main.cs.md)

### Полезные ссылки:  
[Документация по библиотеке NewtonSoft](https://www.newtonsoft.com/json/help/html/Introduction.htm)  
[Парсер JSON](https://jsonformatter.org/json-parser)  
[Конвертор JSON to C#](https://json2csharp.com/)  
[Конвертор JSON to C#](https://app.quicktype.io/)  
[Работа с библиотекой Newtonsoft.Json на реальном примере](https://habr.com/ru/post/482042/)  
[BIML ScriptTaskProject](https://docs.varigence.com/biml/snippets/script-projects-script-task-project)  
[BIML ScriptComponentProject](https://docs.varigence.com/biml/snippets/script-projects-script-component-project)  