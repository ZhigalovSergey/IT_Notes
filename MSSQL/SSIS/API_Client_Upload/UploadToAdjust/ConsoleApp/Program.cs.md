```c#
using Newtonsoft;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Export_adjust
{

    class Program
    {

        static void Main(string[] args)
        {
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string LogFolder = String.Format("C:/Users/zhigalov/Desktop/GOODS/TASKS/4226/VS/log/");
            
            Upload adjust = new Upload();

            try
            {
                //Create Connection to SQL Server in which you like to load files
                string MDWHConnection = String.Format("Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False");
                string QueryString = @"select    [order_id]             -- 0
                                                ,[order_create_dt]      -- 1
                                                ,[order_amount]         -- 2
                                                ,[promocode]            -- 3
                                                ,[android_id]           -- 4
                                                ,[idfa]                 -- 5
                                                ,[need_revenue_event]       -- 6
                                                ,[need_promo_event]         -- 7
                                                ,[android_advertising_id]   -- 8
                                                ,[first_order_flag]         -- 9
                                                ,[orderProductInfo]         -- 10
                                            from interface.adjust_order";

                //Read data from SQL SERVER

                using (OleDbConnection connection = new OleDbConnection(MDWHConnection))
                {
                    Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US", false);
                    OleDbCommand command = new OleDbCommand(QueryString, connection);
                    command.CommandTimeout = 300;
                    connection.Open();
                    OleDbDataReader reader = command.ExecuteReader();
                    // Write All Rows
                    int ColumnCount = reader.FieldCount;
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            //for (int ir = 0; ir < ColumnCount; ir++)
                            //{
                            //    if (!reader.IsDBNull(ir))
                            //    {
                            //        Console.Write(reader.GetValue(ir).ToString());
                            //    }
                            //    if (ir < ColumnCount - 1)
                            //    {
                            //        Console.Write("\t");
                            //    }
                            //}
                            //Console.Write(Environment.NewLine);

                            String order_id = reader["order_id"].ToString();
                            String order_create_dt = reader.GetDateTime(1).ToString("yyyy-MM-ddTHH:mm:ss\\Z+0300");
                            String order_amount = Math.Round((reader.GetDecimal(2) * 1000)).ToString();
                            String android_id = reader.GetValue(4).ToString();
                            String android_advertising_id = reader.GetValue(8).ToString();
                            String idfa = reader.GetValue(5).ToString();

                            // Convert JSON to JSON for Criteo
                            String partner_params = adjust.get_JSON_for_Criteo(reader.GetValue(10).ToString());
                            // Console.WriteLine(partner_params);

                            // {"event_token", "7ge801"}; // событие - order - Успешное оформление заказа
                            if ((Boolean)reader["need_revenue_event"])
                            {
                                adjust.send_revenue_to_adjust(MDWHConnection, order_id, order_create_dt, order_amount, android_id, android_advertising_id, idfa, partner_params);
                                Console.ReadLine();
                            }

                            // {"event_token", "77yg6d"}; // событие - code - Активация промокода (заказ оформленный с промокодом)
                            if ((Boolean)reader["need_promo_event"])
                            {
                                adjust.send_promo_to_adjust(MDWHConnection, order_id, order_create_dt, android_id, android_advertising_id, idfa);
                                Console.ReadLine();
                            }

                            // {event_token", "26j8we"}; // событие - ftb
                            if ((Boolean)reader["first_order_flag"])
                            {
                                adjust.send_first_time_buyer_to_adjust(MDWHConnection, order_id, order_create_dt, android_id, android_advertising_id, idfa);
                                Console.ReadLine();
                            }
                        }
                    }
                    reader.Close();
                }

                Console.ReadLine();
            }
            catch (Exception exception)
            {
                // Create Log File for Errors
                using (StreamWriter sw = File.CreateText(LogFolder
                     + "ErrorLog_" + datetime + ".log"))
                {
                    sw.WriteLine(exception.ToString());
                }
            }

        }
    }
}
```