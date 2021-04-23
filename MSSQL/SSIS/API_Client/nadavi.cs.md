```c#
using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using Newtonsoft;
using Newtonsoft.Json;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{

    [JsonObject]
    public class Click
    {
        [JsonProperty("date")]
        public string Date { get; set; }

        [JsonProperty("account-id")]
        public string AccountId { get; set; }

        [JsonProperty("shop-good-id")]
        public string ShopGoodId { get; set; }

        [JsonProperty("uniq")]
        public string Uniq { get; set; }

        [JsonProperty("good-id")]
        public string GoodId { get; set; }

        [JsonProperty("good-name")]
        public string GoodName { get; set; }

        [JsonProperty("cost")]
        public string Cost { get; set; }

        [JsonProperty("ip")]
        public string IP { get; set; }
    }

    public List<List<Click>> ListListClicks;

    public static List<List<Click>> GetJson(String url, String apikey)
    {

        WebRequest request = WebRequest.Create(url);
        request.Method = "GET";
        request.Headers.Add(apikey);

        // Get the response.
        WebResponse response = request.GetResponse();
        Stream dataStream = response.GetResponseStream();
        StreamReader reader = new StreamReader(dataStream, Encoding.UTF8);
        string result = reader.ReadToEnd();
        response.Close();
        return JsonConvert.DeserializeObject<List<List<Click>>>(result);
    }

    public override void PreExecute()
    {
        base.PreExecute();

        DateTime thisDay = DateTime.Today;
        DateTime lagDay = thisDay.AddDays(-7);
        String apikey = String.Format("ek-apikey: {0}", Variables.apikey);
        String account = Variables.accountid.ToString();

        var Jsons = new List<List<Click>>();

        while (thisDay.Date >= lagDay.Date)
        {
            String url = String.Format("https://nadavi.net/app_api/shop/clicks?account-id={0}&date-from={1}&date-to={2}", account, lagDay.ToString("yyyy-MM-dd"), lagDay.ToString("yyyy-MM-dd"));
            Jsons.AddRange(GetJson(url, apikey));
            lagDay = lagDay.AddDays(1);
        }

        ListListClicks = Jsons;
    }

    public override void PostExecute()
    {
        base.PostExecute();
    }

    public override void CreateNewOutputRows()
    {
        foreach (List<Click> ListClicks in ListListClicks)
        {
            foreach (Click click in ListClicks)
            {
                Output0Buffer.AddRow();
                Output0Buffer.date = click.Date;
                Output0Buffer.accountid = click.AccountId;
                Output0Buffer.shopgoodid = click.ShopGoodId;
                Output0Buffer.uniq = click.Uniq;
                Output0Buffer.goodid = click.GoodId;
                Output0Buffer.goodname = click.GoodName;
                Output0Buffer.cost = click.Cost;
                Output0Buffer.ip = click.IP;
            }
        }
    }

}
```