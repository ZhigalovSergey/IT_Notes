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
using System.Diagnostics;
using System.IO;
using Renci.SshNet;
#endregion

namespace ST_9d1b2b00cbb84f53bcbe2173ec3851c7
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
            string DestinationFolder = String.Format("{0}/gfk/temp/", Dts.Variables["WorkingDirectory"].Value);
            string LogFolder = String.Format("{0}/gfk/log/", Dts.Variables["WorkingDirectory"].Value);
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");

            //Create Connection to SQL Server in which you like to load files
            string MDWHConnection = "Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False";

            //Delete DestinationFolder
            if (Directory.Exists(DestinationFolder))
            {
                Directory.Delete(DestinationFolder, true);
            }

            try
            {
                DateTime thisDay = DateTime.Today;
                DateTime SunDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek);
                DateTime MonDay = thisDay.AddDays((int)DayOfWeek.Sunday - (int)DateTime.Today.DayOfWeek - 6);
                string date_from = MonDay.ToString("yyyyMMdd");
                string date_to = SunDay.ToString("yyyyMMdd");

                //Declare Variables and provide values
                string FileDelimiter = "\t";        //You can provide comma or pipe or whatever you like
                string FileExtension = ".txt";      //Provide the extension you like such as .txt or .csv

                //Create DestinationFolder
                if (!Directory.Exists(DestinationFolder))
                {
                    Directory.CreateDirectory(DestinationFolder);
                }

                //Read data from SQL SERVER
                Export ex = new Export();
                string FileNamePart = String.Format("GOODS_ADDRESS_{0}_{1}", date_from, date_to);
                string QueryString = String.Format("exec interface.gfk_cities");
                ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

                FileNamePart = String.Format("GOODS_MAPPING_{0}_{1}", date_from, date_to);
                QueryString = String.Format("exec interface.gfk_weekly_assortment {0}, {1}", date_from, date_to);
                ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

                FileNamePart = String.Format("GOODS_{0}_{1}", date_from, date_to);
                QueryString = String.Format("exec interface.gfk_orders {0}, {1}", date_from, date_to);
                ex.Export_to_flat_file(MDWHConnection, QueryString, DestinationFolder + FileNamePart + FileExtension, FileDelimiter);

                //Create zip file
                string SourceName = String.Format("{0}*.*", DestinationFolder);
                string TargetName = String.Format("{0}GOODS_{1}_{2}.zip", DestinationFolder, date_from, date_to);
                CreateZip(SourceName, TargetName);

                //Copy zip file
                string CopyName = String.Format("{0}/gfk/upload_to_gfk/GOODS_{1}_{2}.zip", Dts.Variables["WorkingDirectory"].Value, date_from, date_to);
                File.Copy(TargetName, CopyName, true);

                //Upload zip file to GFK
                string password = Dts.Variables["gfk_pass"].GetSensitiveValue().ToString();
                PrivateKeyFile keyFile = new PrivateKeyFile(String.Format("{0}{1}", Dts.Variables["WorkingDirectory"].Value, Dts.Variables["PrivateKeyFilePath"].Value));
                string LocalPath = DestinationFolder;
                string RemotePath = @"/";

                Upload upload = new Upload();
                upload.Upload_to_GFK(keyFile, password, LocalPath, RemotePath);

                //Delete DestinationFolder
                Directory.Delete(DestinationFolder, true);

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

                Dts.Events.FireError(0, "Script Task - Upload to GFK", "An error occurred in Script Task - Upload to GFK: " + exception.Message.ToString(), "", 0);

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