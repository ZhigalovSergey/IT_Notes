```c#
#region Help:  Introduction to the script task
/* The Script Task allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services control flow. 
 * 
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script task. */
#endregion


#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Data.OleDb;
using System.Threading;
using System.Globalization;
using System.IO;
using System.Net;
#endregion

namespace ST_d9095ca136244bee885a71b44a0c62c4
{
    /// <summary>
    /// ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    /// or parent of this class.
    /// </summary>
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
	public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
	{
        #region Help:  Using Integration Services variables and parameters in a script
        /* To use a variable in this script, first ensure that the variable has been added to 
         * either the list contained in the ReadOnlyVariables property or the list contained in 
         * the ReadWriteVariables property of this script task, according to whether or not your
         * code needs to write to the variable.  To add the variable, save this script, close this instance of
         * Visual Studio, and update the ReadOnlyVariables and 
         * ReadWriteVariables properties in the Script Transformation Editor window.
         * To use a parameter in this script, follow the same steps. Parameters are always read-only.
         * 
         * Example of reading from a variable:
         *  DateTime startTime = (DateTime) Dts.Variables["System::StartTime"].Value;
         * 
         * Example of writing to a variable:
         *  Dts.Variables["User::myStringVariable"].Value = "new value";
         * 
         * Example of reading from a package parameter:
         *  int batchId = (int) Dts.Variables["$Package::batchId"].Value;
         *  
         * Example of reading from a project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].Value;
         * 
         * Example of reading from a sensitive project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].GetSensitiveValue();
         * */

        #endregion

        #region Help:  Firing Integration Services events from a script
        /* This script task can fire events for logging purposes.
         * 
         * Example of firing an error event:
         *  Dts.Events.FireError(18, "Process Values", "Bad value", "", 0);
         * 
         * Example of firing an information event:
         *  Dts.Events.FireInformation(3, "Process Values", "Processing has started", "", 0, ref fireAgain)
         * 
         * Example of firing a warning event:
         *  Dts.Events.FireWarning(14, "Process Values", "No values received for input", "", 0);
         * */
        #endregion

        #region Help:  Using Integration Services connection managers in a script
        /* Some types of connection managers can be used in this script task.  See the topic 
         * "Working with Connection Managers Programatically" for details.
         * 
         * Example of using an ADO.Net connection manager:
         *  object rawConnection = Dts.Connections["Sales DB"].AcquireConnection(Dts.Transaction);
         *  SqlConnection myADONETConnection = (SqlConnection)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Sales DB"].ReleaseConnection(rawConnection);
         *
         * Example of using a File connection manager
         *  object rawConnection = Dts.Connections["Prices.zip"].AcquireConnection(Dts.Transaction);
         *  string filePath = (string)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Prices.zip"].ReleaseConnection(rawConnection);
         * */
        #endregion


		/// <summary>
        /// This method is called when this script task executes in the control flow.
        /// Before returning from this method, set the value of Dts.TaskResult to indicate success or failure.
        /// To open Help, press F1.
        /// </summary>
		public void Main()
		{
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string LogFolder = Dts.Variables["LogFolder"].Value.ToString();
            string MDWHConnection = String.Format("Data Source={0};Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False", "dwh.prod.lan"); // dwh.prod.lan, dwh.dev.lan

            Upload adjust = new Upload();
            adjust.LogFolder = LogFolder;
            adjust.Connection = MDWHConnection;
            adjust.app_token = Dts.Variables["adjust_app_token"].GetSensitiveValue().ToString();
            adjust.environment = "production"; // "production" - for prod, "sandbox" - for testing

            Int16 cnt = 0;
            try
            {
                //Create Connection to SQL Server in which you like to load files
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
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            String order_id = reader["order_id"].ToString();
                            String order_create_dt = reader.GetDateTime(1).ToString("yyyy-MM-ddTHH:mm:ss\\Z+0300");
                            String order_amount = Math.Round((reader.GetDecimal(2) * 1000)).ToString();
                            String android_id = reader.GetValue(4).ToString();
                            String android_advertising_id = reader.GetValue(8).ToString();
                            String idfa = reader.GetValue(5).ToString();

                            // Convert JSON to JSON for Criteo
                            String partner_params = adjust.get_JSON_for_Criteo(reader.GetValue(10).ToString());

                            // {"event_token", "7ge801"}; // событие - order
                            if ((Boolean)reader["need_revenue_event"])
                            {
                                adjust.send_revenue_to_adjust(order_id, order_create_dt, order_amount, android_id, android_advertising_id, idfa, partner_params);
                            }

                            // {"event_token", "77yg6d"}; // событие - code
                            if ((Boolean)reader["need_promo_event"])
                            {
                                adjust.send_promo_to_adjust(order_id, order_create_dt, android_id, android_advertising_id, idfa);
                            }

                            // {event_token", "26j8we"}; // событие - ftb
                            if ((Boolean)reader["first_order_flag"])
                            {
                                adjust.send_first_time_buyer_to_adjust(order_id, order_create_dt, android_id, android_advertising_id, idfa);
                            }

                            cnt += 1;
                        }
                    }
                    reader.Close();
                }

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (WebException exception)
            {
                string msg;
                using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                {
                    msg = String.Format("An error occurred in Script Task - Upload to Adjust: {0}", exception.Message.ToString());
                    msg = msg + String.Format("\r\nException.Status is {0}", exception.Status.ToString());
                    msg = msg + String.Format("\r\nWas transferred {0} orders", cnt.ToString());
                    sw.Write(msg);
                }

                if (exception.Status == WebExceptionStatus.ProtocolError)
                {
                    WebResponse resp = exception.Response;
                    using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                    {
                        using (StreamWriter sw = File.AppendText(LogFolder + "AdjustErrorLog_" + datetime + ".log"))
                        {
                            msg = msg + String.Format("\r\n{0}", sr.ReadToEnd());
                            sw.WriteLine(sr.ReadToEnd());
                        }
                    }
                }

                msg = msg + String.Format("\r\nPath LogFolder is {0}", LogFolder);
                Dts.Events.FireError(0, "Script Task - Upload to Adjust", msg, "", 0);
                //Dts.TaskResult = (int)ScriptResults.Failure;
            }
            catch (Exception exception)
            {
                // Create Log File for Errors
                using (StreamWriter sw = File.CreateText(LogFolder
                     + "ErrorLog_" + datetime + ".log"))
                {
                    sw.WriteLine(exception.ToString());
                    sw.WriteLine("Was transferred {0} orders", cnt.ToString());
                }

                Dts.Events.FireError(0, "Script Task - Upload to Adjust", "An error occurred in Script Task - Upload to Adjust: " + exception.Message.ToString(), "", 0);
                //Dts.TaskResult = (int)ScriptResults.Failure;
            }
            
		}

        #region ScriptResults declaration
        /// <summary>
        /// This enum provides a convenient shorthand within the scope of this class for setting the
        /// result of the script.
        /// 
        /// This code was generated automatically.
        /// </summary>
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

	}
}
```