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
using System.IO;
using System.Diagnostics;
using Renci.SshNet;
#endregion

namespace ST_ae54ed5b852743dcb0d5effb6eeb6bd2
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
            string DestinationFolder = String.Format("{0}/nielsen/temp/", Dts.Variables["WorkingDirectory"].Value);
            string LogFolder = String.Format("{0}/nielsen/log/", Dts.Variables["WorkingDirectory"].Value);
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            Int32 TimeOut = (Int32)Dts.Variables["TimeOut"].Value;
            //Create Connection to SQL Server
            string MDWHConnection = "Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False";

            //Delete DestinationFolder
            if (Directory.Exists(DestinationFolder))
            {
                Directory.Delete(DestinationFolder, true);
            }

            try
            {
                DateTime thisDay = DateTime.Today;
                DateTime StartDate;
                String ArchiveStartDate = Dts.Variables["ArchiveStartDate"].Value.ToString();
                try
                {
                    StartDate = DateTime.ParseExact(ArchiveStartDate, "yyyy.MM.dd", null);
                }
                catch
                {
                    StartDate = thisDay;
                }

                while ((thisDay - StartDate).Days >= 0)
                {
                    DateTime SunDay = StartDate.AddDays((int)DayOfWeek.Sunday - (int)StartDate.DayOfWeek);
                    DateTime MonDay = StartDate.AddDays((int)DayOfWeek.Sunday - (int)StartDate.DayOfWeek - 6);
                    string date_from = MonDay.ToString("yyyyMMdd");
                    string date_to = SunDay.ToString("yyyyMMdd");

                    //Declare Variables and provide values
                    string FileDelimiter = "\t";        //You can provide comma or pipe or whatever you like
                    string FileExtension = ".csv";      //Provide the extension you like such as .txt or .csv

                    //Create DestinationFolder
                    Directory.CreateDirectory(DestinationFolder);

                    //Read data from SQL SERVER
                    Export ex = new Export();

                    string FileNamePart = String.Format("GOODS_Merchants_{0}_{1}", date_from, date_to);
                    string QueryString = String.Format("exec interface.nielsen_merchants {0}, {1}", date_from, date_to);
                    ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter, TimeOut);

                    FileNamePart = String.Format("GOODS_Orders_{0}_{1}", date_from, date_to);
                    QueryString = String.Format("exec interface.nielsen_orders {0}, {1}", date_from, date_to);
                    ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter, TimeOut);

                    FileNamePart = String.Format("GOODS_Sales_{0}_{1}", date_from, date_to);
                    QueryString = String.Format("exec interface.nielsen_sales {0}, {1}", date_from, date_to);
                    ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter, TimeOut);

                    FileNamePart = String.Format("GOODS_Mapping_{0}_{1}", date_from, date_to);
                    QueryString = String.Format("exec interface.nielsen_mapping {0}, {1}", date_from, date_to);
                    ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter, TimeOut);

                    FileNamePart = String.Format("GOODS_Addresses_{0}_{1}", date_from, date_to);
                    QueryString = String.Format("exec interface.nielsen_addresses {0}, {1}", date_from, date_to);
                    ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter, TimeOut);

                    //Create zip file
                    string SourceName = String.Format("{0}*.*", DestinationFolder);
                    string TargetName = String.Format("{0}{1}{2}_{3}.zip", DestinationFolder, Dts.Variables["ArchiveTemplate"].Value, date_from, date_to);
                    CreateZip(SourceName, TargetName);

                    //Copy zip file
                    string CopyName = String.Format("{0}/nielsen/Upload_to_Nielsen/{1}{2}_{3}.zip", Dts.Variables["WorkingDirectory"].Value, Dts.Variables["ArchiveTemplate"].Value, date_from, date_to);
                    File.Copy(TargetName, CopyName, true);

                    //Upload zip file to Nielsen
                    PrivateKeyFile keyFile = new PrivateKeyFile(String.Format("{0}{1}", Dts.Variables["WorkingDirectory"].Value, Dts.Variables["PrivateKeyFilePath"].Value));
                    string LocalPath = String.Format("{0}", DestinationFolder);
                    string RemotePath = "/Upload_to_Nielsen/";

                    Upload upload = new Upload();
                    upload.Upload_to_Nielson(keyFile, LocalPath, RemotePath);

                    //Delete DestinationFolder
                    Directory.Delete(DestinationFolder, true);

                    StartDate = StartDate.AddDays(7);
                }

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception exception)
            {
                // Create Log File for Errors
                using (StreamWriter sw = File.CreateText(LogFolder
                    + "\\" + "ErrorLog_" + datetime + ".log"))
                {
                    sw.WriteLine(exception.ToString());
                }

                //Delete DestinationFolder
                if (Directory.Exists(DestinationFolder))
                {
                    Directory.Delete(DestinationFolder, true);
                }

                Dts.Events.FireError(0, "Script Task - Upload to Nielsen", "An error occurred in Script Task - Upload to Nielsen: " + exception.Message.ToString(), "", 0);

                Dts.TaskResult = (int)ScriptResults.Failure;
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