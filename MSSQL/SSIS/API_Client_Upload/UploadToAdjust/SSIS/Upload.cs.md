```c#
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ST_d9095ca136244bee885a71b44a0c62c4
{
    class Upload
    {
        // https://help.adjust.com/en/article/raw-data-exports
        // https://docs.adjust.com/ru/
        // https://conf.goods.ru/pages/viewpage.action?pageId=139170657
        // https://conf.goods.ru/pages/viewpage.action?pageId=75910662

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

        public string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        public string LogFolder { get; set; }
        public string Connection { get; set; }
        public string app_token { get; set; }
        public string environment { get; set; }  // "production" - for prod, "sandbox" - for testing
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
        public void Logging_order(string order_id, string event_type)
        {
            // В будущем нужно выделить adjust в отдельный лог orders_output_adjust_log - ДЛЯ УСКОРЕНИЯ РАБОТЫ
            using (OleDbConnection connection = new OleDbConnection(Connection))
            {
                string QueryString = String.Format("exec interface.add_orders_output_log {0}, \"{1}\"", order_id, event_type);
                OleDbCommand command = new OleDbCommand(QueryString, connection);
                connection.Open();
                Int32 row_affected = command.ExecuteNonQuery();
            }
        }

        public void send_revenue_to_adjust(string order_id, string create_dt, string revenue, string android_id, string android_advertising_id, string idfa, string partner_params)
        {

            string url = "https://app.adjust.com/revenue";
            WebRequest request = WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = "application/json";
            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "7ge801"); // событие - order
            data.Add("s2s", "1");
            data.Add("created_at", create_dt);
            data.Add("amount", revenue);
            data.Add("currency", "RUB");
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing
            data.Add("partner_params", partner_params);

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
            byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

            request.ContentLength = postBytes.Length;
            request.GetRequestStream().Write(postBytes, 0, postBytes.Length);
            request.GetRequestStream().Close();

            // Get the response.
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Logging_order(order_id, "Adjust revenue");
                }
                else
                {
                    using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                    {
                        sw.WriteLine("\r\nPOST [Adjust revenue] is {0} and order_id is {1}", response.StatusCode, order_id);
                        sw.WriteLine("\r\nPOST is {0}", RequestContent);
                    }
                }
            }
        }

        public void send_promo_to_adjust(string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
        {
            string url = "https://app.adjust.com/event";
            WebRequest request = WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = "application/json";
            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "77yg6d"); // событие - code
            data.Add("s2s", "1");
            data.Add("created_at", create_dt);
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
            byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

            request.ContentLength = postBytes.Length;
            request.GetRequestStream().Write(postBytes, 0, postBytes.Length);
            request.GetRequestStream().Close();

            // Get the response.
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Logging_order(order_id, "Adjust promo");
                }
                else
                {
                    using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                    {
                        sw.WriteLine("\r\nPOST [Adjust promo] is {0} and order_id is {1}", response.StatusCode, order_id);
                        sw.WriteLine("\r\nPOST is {0}", RequestContent);
                    }
                }
            }
        }


        public void send_first_time_buyer_to_adjust(string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
        {
            string url = "https://app.adjust.com/event";
            WebRequest request = WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = "application/json";
            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "26j8we"); // событие - ftb
            data.Add("s2s", "1");
            data.Add("created_at", create_dt);
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            string RequestContent = JsonConvert.SerializeObject(data, Formatting.Indented);
            byte[] postBytes = Encoding.ASCII.GetBytes(RequestContent);

            request.ContentLength = postBytes.Length;
            request.GetRequestStream().Write(postBytes, 0, postBytes.Length);
            request.GetRequestStream().Close();

            // Get the response.
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // Logging_order(order_id, "Adjust FTB");
                }
                else
                {
                    using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                    {
                        sw.WriteLine("\r\nPOST [Adjust FTB] is {0} and order_id is {1}", response.StatusCode, order_id);
                        sw.WriteLine("\r\nPOST is {0}", RequestContent);
                    }
                }
            }
        }

    }
}
```