```c#
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


class Upload
{
    // https://help.adjust.com/en/article/raw-data-exports
    // https://docs.adjust.com/ru/
	// 

    [JsonObject]
    public class OrderProductInfo
    {
        [JsonProperty("orderid")]
        public string Orderid { get; set; }

        [JsonProperty("productid")]
        public string Productid { get; set; }

        [JsonProperty("quantity")]
        public string Quantity { get; set; }

        [JsonProperty("price")]
        public string Price { get; set; }

        [JsonProperty("criteo_email_hash")]
        public string CriteoEmailHash { get; set; }
    }

    public string APP_TOKEN = "";
    public static List<OrderProductInfo> ListProducts;

    //Convert JSON to JSON for Criteo
    public string get_JSON_for_Criteo(string orderProductInfo)
    {
        ListProducts = JsonConvert.DeserializeObject<List<OrderProductInfo>>(orderProductInfo);
        List<String> order_id = new List<String>();
        List<String> product_id = new List<String>();
        List<String> price = new List<String>();
        List<String> quantity = new List<String>();
        List<String> criteo_email_hash = new List<String>();
        foreach (OrderProductInfo product in ListProducts)
        {
            order_id.Add(product.Orderid);
            product_id.Add(product.Productid);
            price.Add(product.Quantity);
            quantity.Add(product.Price);
            criteo_email_hash.Add(product.CriteoEmailHash);
        }
        Dictionary<string, string> dict_params = new Dictionary<string, string>();
        dict_params.Add("orderid", String.Join(",", order_id.ToArray()));
        dict_params.Add("productid", String.Join(",", product_id.ToArray()));
        dict_params.Add("price", String.Join(",", price.ToArray()));
        dict_params.Add("quantity", String.Join(",", quantity.ToArray()));
        dict_params.Add("criteo_email_hash", String.Join(",", criteo_email_hash.ToArray()));
        return JsonConvert.SerializeObject(dict_params, Formatting.Indented);
        
    }

    //Logging the upload order to SQL SERVER
    public void Logging_order(string Connection, string order_id, string event_type)
    {
        using (OleDbConnection connection = new OleDbConnection(Connection))
        {
            string QueryString = String.Format("exec interface.add_orders_output_log {0}, \"{1}\"", order_id, event_type);
            OleDbCommand command = new OleDbCommand(QueryString, connection);
            connection.Open();
            Int32 row_affected = command.ExecuteNonQuery();
            Console.WriteLine(QueryString);
            Console.WriteLine("row_affected: " + row_affected.ToString());
            //Console.ReadLine();
        }
    }

    public void send_revenue_to_adjust(string Connection, string order_id, string create_dt, string revenue, string android_id, string android_advertising_id, string idfa, string partner_params)
    {
        string url = "https://app.adjust.com/revenue";
        WebRequest request = WebRequest.Create(url);
        request.Method = "POST";
        request.ContentType = "application/json";
        Dictionary<string, string> data = new Dictionary<string, string>();

        data.Add("app_token", APP_TOKEN);
        data.Add("event_token", "7ge801"); // событие - order
        data.Add("s2s", "1");
        data.Add("created_at", create_dt);
        data.Add("amount", revenue);
        data.Add("currency", "RUB");
        data.Add("environment", "production"); // "production" - for prod, "sandbox" - for testing
        data.Add("partner_params", partner_params);

        if (android_id.Length > 0) { data.Add("android_id", android_id); }
        if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
        if (idfa.Length > 0) { data.Add("idfa", idfa); }

        string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
        Console.WriteLine(RequestContent);

        byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

        request.ContentLength = postBytes.Length;
        request.GetRequestStream().Write(postBytes, 0, postBytes.Length);

        // Get the response.
        HttpWebResponse response = (HttpWebResponse)request.GetResponse();
        Console.WriteLine((int)response.StatusCode);
        response.Close();
        if (response.StatusCode == HttpStatusCode.OK)
            { 
                Console.WriteLine("\r\nPOST [Adjust revenue] is OK and order_id is {0}", order_id);
                Logging_order(Connection, order_id, "Adjust revenue");
            }
        
    }

    public void send_promo_to_adjust(string Connection, string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
    {
        string url = "https://app.adjust.com/event";
        WebRequest request = WebRequest.Create(url);
        request.Method = "POST";
        request.ContentType = "application/json";
        Dictionary<string, string> data = new Dictionary<string, string>();

        data.Add("app_token", APP_TOKEN);
        data.Add("event_token", "77yg6d"); // событие - code
        data.Add("s2s", "1");
        data.Add("created_at", create_dt);
        data.Add("environment", "production"); // "production" - for prod, "sandbox" - for testing

        if (android_id.Length > 0) { data.Add("android_id", android_id); }
        if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
        if (idfa.Length > 0) { data.Add("idfa", idfa); }

        string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
        Console.WriteLine(RequestContent);

        byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

        request.ContentLength = postBytes.Length;
        request.GetRequestStream().Write(postBytes, 0, postBytes.Length);

        // Get the response.
        HttpWebResponse response = (HttpWebResponse)request.GetResponse();
        Console.WriteLine((int)response.StatusCode);
        response.Close();
        if (response.StatusCode == HttpStatusCode.OK)
        {
            Console.WriteLine("\r\nPOST [Adjust promo] is OK and order_id is {0}", order_id);
            Logging_order(Connection, order_id, "Adjust promo");
        }
        
    }


    public void send_first_time_buyer_to_adjust(string Connection, string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
    {
        string url = "https://app.adjust.com/event";
        WebRequest request = WebRequest.Create(url);
        request.Method = "POST";
        request.ContentType = "application/json";
        Dictionary<string, string> data = new Dictionary<string, string>();

        data.Add("app_token", APP_TOKEN);
        data.Add("event_token", "26j8we"); // событие - ftb
        data.Add("s2s", "1");
        data.Add("created_at", create_dt);
        data.Add("environment", "production"); // "production" - for prod, "sandbox" - for testing

        if (android_id.Length > 0) { data.Add("android_id", android_id); }
        if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
        if (idfa.Length > 0) { data.Add("idfa", idfa); }

        string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
        Console.WriteLine(RequestContent);

        byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

        request.ContentLength = postBytes.Length;
        request.GetRequestStream().Write(postBytes, 0, postBytes.Length);

        // Get the response.
        HttpWebResponse response = (HttpWebResponse)request.GetResponse();
        Console.WriteLine((int)response.StatusCode);
        response.Close();
        if (response.StatusCode == HttpStatusCode.OK)
        {
            Console.WriteLine("\r\nPOST [Adjust FTB] is OK and order_id is {0}", order_id);
            // Logging_order(Connection, order_id, "Adjust FTB");
        }
        
    }

}
```