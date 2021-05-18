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
        // https://help.adjust.com/en/article/encoding

        /*
        
         Callback`и с Adjust сохраняются 
                    в таблицу [AIS].[AIS].[adjust_event] на сервере AIS-SDB-003
                    с помощью сервиса adjustIntegrationService (https://prod-api.goods.ru/api/market/v1/adjustIntegrationService/event/save)
                    Этот сервис имеет один метод GET /api/market/v1/adjustIntegrationService/event/save и в query принимает параметр eventName
                    остальные параметры кладутся в поле [AIS].[ais].[adjust_event].[data]

                Для тестирования
         https://prod-api.goods.ru/api/market/v1/adjustIntegrationService/event/save?eventName=order&greeting=Hello

        SELECT TOP(1000) [created_at]
            ,[event_name]
            ,[data]
            ,[id]
            ,[is_sent_to_adjust]
        FROM [AIS].[ais].[adjust_event]
        where [event_name] = 'order'
        and [created_at] >= dateadd(MI, -10, sysdatetime())
        order by [created_at] desc

        "created_at" возвращается в Unix формате - поле [data] тег "timestamp"
        select dateadd(S, 1620922866, '1970-01-01')

        */


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
        public Int32  TimeOut { get; set; }
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
                quantity.Add(product.Quantity);
                price.Add(product.Price);
                criteo_email_hash.Add(product.CriteoEmailHash);
            }
            Dictionary<string, string> dict_params = new Dictionary<string, string>();
            dict_params.Add("orderid", WebUtility.UrlEncode(order_id[0]));
            dict_params.Add("productid", WebUtility.UrlEncode(String.Join(",", product_id.ToArray())));
            dict_params.Add("price", WebUtility.UrlEncode(String.Join(",", price.ToArray())));
            dict_params.Add("quantity", WebUtility.UrlEncode(String.Join(",", quantity.ToArray())));
            dict_params.Add("criteo_email_hash", WebUtility.UrlEncode(String.Join(",", criteo_email_hash.ToArray())));
            return "{" + string.Join(",", dict_params.Select(kvp => string.Format("\"{0}\":\"{1}\"", kvp.Key, kvp.Value))) + "}";

        }

        //Logging the upload order to SQL SERVER
        public void Logging_order(string order_id, string event_type, string status_code)
        {
            // В будущем нужно выделить adjust в отдельный лог orders_output_adjust_log - ДЛЯ УСКОРЕНИЯ РАБОТЫ
            using (OleDbConnection connection = new OleDbConnection(Connection))
            {
                string QueryString = String.Format("exec interface.add_orders_output_log {0}, \"{1}\", {2}", order_id, event_type, status_code);
                OleDbCommand command = new OleDbCommand(QueryString, connection);
                command.CommandTimeout = TimeOut;
                connection.Open();
                Int32 row_affected = command.ExecuteNonQuery();
            }
        }

        public Int16 send_revenue_to_adjust(Int16 cnt, string order_id, string create_dt, string revenue, string android_id, string android_advertising_id, string idfa, string partner_params)
        {
            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "7ge801"); // событие - order
            data.Add("s2s", "1");
            data.Add("created_at_unix", create_dt);
            data.Add("amount", revenue);
            data.Add("currency", "RUB");
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            data.Add("callback_params", partner_params);
            data.Add("partner_params", partner_params);

            string JSONContent = JsonConvert.SerializeObject(data, Formatting.Indented);

            string RequestContent = string.Join("&", data.Select(kvp => string.Format("{0}={1}", kvp.Key, kvp.Value)));
            string url = "https://app.adjust.com/revenue";
            string event_url = String.Format("{0}?{1}", url, RequestContent);
            WebRequest request = WebRequest.Create(event_url);
            request.Method = "POST";

            // Get the response.
            try
            {
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        Logging_order(order_id, "Adjust revenue", "OK");
                        //using (StreamWriter sw = File.AppendText(LogFolder + "TransferredToAdjust_" + datetime + ".log"))
                        //{
                        //    sw.WriteLine("\r\nPOST [Adjust revenue] for order_id: {0}", order_id);
                        //    sw.WriteLine("\r\nPOST is {0}", event_url);
                        //}
                        cnt += 1;
                    }
                }
            }
            catch (WebException exception)
            {
                string msg;
                msg = String.Format("\r\nOrder_id is \r\n{0}\r\n", order_id);
                msg = msg + String.Format("\r\nEvent is Promo\r\n", order_id);
                msg = msg + String.Format("\r\nJSONContent is \r\n{0} \r\n", JSONContent);
                msg = msg + String.Format("\r\nPOST [Adjust revenue] is \r\n{0} \r\n", event_url);
                msg = msg + String.Format("\r\nAn error occurred in Script Task - Upload to Adjust: {0}", exception.Message.ToString());
                msg = msg + String.Format("\r\nException.Status is {0}", exception.Status.ToString());

                string StatusCode = "";
                if (exception.Status == WebExceptionStatus.ProtocolError)
                {
                    HttpWebResponse resp = (HttpWebResponse)exception.Response;
                    StatusCode = resp.StatusCode.ToString();
                    msg = msg + String.Format("\r\nResponse.StatusCode is {0} - {1}", ((int)resp.StatusCode).ToString(), resp.StatusCode.ToString());
                    msg = msg + String.Format("\r\nResponse.StatusDescription is {0}", resp.StatusDescription.ToString());
                    using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                    {
                        msg = msg + String.Format("\r\nResponse is {0} \r\n", sr.ReadToEnd());
                    }
                }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write(msg);
                }

                if (StatusCode != "NotFound" && StatusCode != "BadRequest")
                { throw exception; }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write("\r\n----------------------------------------------------\r\n");
                }

                Logging_order(order_id, "Adjust revenue", StatusCode);
            }
            return cnt;
        }

        public Int16 send_promo_to_adjust(Int16 cnt, string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
        {

            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "77yg6d"); // событие - code
            data.Add("s2s", "1");
            data.Add("created_at_unix", create_dt);
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            string JSONContent = JsonConvert.SerializeObject(data, Formatting.Indented);

            string RequestContent = string.Join("&", data.Select(kvp => string.Format("{0}={1}", kvp.Key, kvp.Value)));
            string url = "https://app.adjust.com/event";
            string event_url = String.Format("{0}?{1}", url, RequestContent);
            WebRequest request = WebRequest.Create(event_url);
            request.Method = "POST";

            // Get the response.
            try
            {
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        Logging_order(order_id, "Adjust promo", "OK");
                        cnt += 1;
                    }
                }
            }
            catch (WebException exception)
            {
                string msg;
                msg = String.Format("\r\nOrder_id is \r\n{0}\r\n", order_id);
                msg = msg + String.Format("\r\nEvent is Promo\r\n", order_id);
                msg = msg + String.Format("\r\nJSONContent is \r\n{0} \r\n", JSONContent);
                msg = msg + String.Format("\r\nPOST [Adjust revenue] is \r\n{0} \r\n", event_url);
                msg = msg + String.Format("\r\nAn error occurred in Script Task - Upload to Adjust: {0}", exception.Message.ToString());
                msg = msg + String.Format("\r\nException.Status is {0}", exception.Status.ToString());

                string StatusCode = "";
                if (exception.Status == WebExceptionStatus.ProtocolError)
                {
                    HttpWebResponse resp = (HttpWebResponse)exception.Response;
                    StatusCode = resp.StatusCode.ToString();
                    msg = msg + String.Format("\r\nResponse.StatusCode is {0} - {1}", ((int)resp.StatusCode).ToString(), resp.StatusCode.ToString());
                    msg = msg + String.Format("\r\nResponse.StatusDescription is {0}", resp.StatusDescription.ToString());
                    using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                    {
                        msg = msg + String.Format("\r\nResponse is {0} \r\n", sr.ReadToEnd());
                    }
                }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write(msg);
                }

                if (StatusCode != "NotFound" && StatusCode != "BadRequest")
                { throw exception; }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write("\r\n----------------------------------------------------\r\n");
                }

                Logging_order(order_id, "Adjust promo", StatusCode);
            }
            return cnt;
        }


        public Int16 send_first_time_buyer_to_adjust(Int16 cnt, string order_id, string create_dt, string android_id, string android_advertising_id, string idfa)
        {
            Dictionary<string, string> data = new Dictionary<string, string>();

            data.Add("app_token", app_token);
            data.Add("event_token", "26j8we"); // событие - ftb
            data.Add("s2s", "1");
            data.Add("created_at_unix", create_dt);
            data.Add("environment", environment); // "production" - for prod, "sandbox" - for testing

            if (android_id.Length > 0) { data.Add("android_id", android_id); }
            if (android_advertising_id.Length > 0) { data.Add("gps_adid", android_advertising_id); }
            if (idfa.Length > 0) { data.Add("idfa", idfa); }

            string JSONContent = JsonConvert.SerializeObject(data, Formatting.Indented);

            string RequestContent = string.Join("&", data.Select(kvp => string.Format("{0}={1}", kvp.Key, kvp.Value)));
            string url = "https://app.adjust.com/event";
            string event_url = String.Format("{0}?{1}", url, RequestContent);
            WebRequest request = WebRequest.Create(event_url);
            request.Method = "POST";

            // Get the response.
            try
            {
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        // Logging_order(order_id, "Adjust FTB", "OK");
                        cnt += 1;
                    }
                }
            }
            catch (WebException exception)
            {
                string msg;
                msg = String.Format("\r\nOrder_id is \r\n{0}\r\n", order_id);
                msg = msg + String.Format("\r\nEvent is FTB\r\n", order_id);
                msg = msg + String.Format("\r\nJSONContent is \r\n{0} \r\n", JSONContent);
                msg = msg + String.Format("\r\nPOST [Adjust revenue] is \r\n{0} \r\n", event_url);
                msg = msg + String.Format("\r\nAn error occurred in Script Task - Upload to Adjust: {0}", exception.Message.ToString());
                msg = msg + String.Format("\r\nException.Status is {0}", exception.Status.ToString());

                string StatusCode = "";
                if (exception.Status == WebExceptionStatus.ProtocolError)
                {
                    HttpWebResponse resp = (HttpWebResponse)exception.Response;
                    StatusCode = resp.StatusCode.ToString();
                    msg = msg + String.Format("\r\nResponse.StatusCode is {0} - {1}", ((int)resp.StatusCode).ToString(), resp.StatusCode.ToString());
                    msg = msg + String.Format("\r\nResponse.StatusDescription is {0}", resp.StatusDescription.ToString());
                    using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                    {
                        msg = msg + String.Format("\r\nResponse is {0} \r\n", sr.ReadToEnd());
                    }
                }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write(msg);
                }

                if (StatusCode != "NotFound" && StatusCode != "BadRequest")
                { throw exception; }

                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    sw.Write("\r\n----------------------------------------------------\r\n");
                }

                Logging_order(order_id, "Adjust FTB", StatusCode);
            }
            return cnt;
        }

    }
}
```