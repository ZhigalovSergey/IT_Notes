```c#
#region Help:  Introduction to the Script Component
/* The Script Component allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services data flow.
 *
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script component. */
#endregion

#region Namespaces
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
#endregion

/// <summary>
/// This is the class to which to add your code.  Do not change the name, attributes, or parent
/// of this class.
/// </summary>
[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    #region Help:  Using Integration Services variables and parameters
    /* To use a variable in this script, first ensure that the variable has been added to
     * either the list contained in the ReadOnlyVariables property or the list contained in
     * the ReadWriteVariables property of this script component, according to whether or not your
     * code needs to write into the variable.  To do so, save this script, close this instance of
     * Visual Studio, and update the ReadOnlyVariables and ReadWriteVariables properties in the
     * Script Transformation Editor window.
     * To use a parameter in this script, follow the same steps. Parameters are always read-only.
     *
     * Example of reading from a variable or parameter:
     *  DateTime startTime = Variables.MyStartTime;
     *
     * Example of writing to a variable:
     *  Variables.myStringVariable = "new value";
     */
    #endregion

    #region Help:  Using Integration Services Connnection Managers
    /* Some types of connection managers can be used in this script component.  See the help topic
     * "Working with Connection Managers Programatically" for details.
     *
     * To use a connection manager in this script, first ensure that the connection manager has
     * been added to either the list of connection managers on the Connection Managers page of the
     * script component editor.  To add the connection manager, save this script, close this instance of
     * Visual Studio, and add the Connection Manager to the list.
     *
     * If the component needs to hold a connection open while processing rows, override the
     * AcquireConnections and ReleaseConnections methods.
     * 
     * Example of using an ADO.Net connection manager to acquire a SqlConnection:
     *  object rawConnection = Connections.SalesDB.AcquireConnection(transaction);
     *  SqlConnection salesDBConn = (SqlConnection)rawConnection;
     *
     * Example of using a File connection manager to acquire a file path:
     *  object rawConnection = Connections.Prices_zip.AcquireConnection(transaction);
     *  string filePath = (string)rawConnection;
     *
     * Example of releasing a connection manager:
     *  Connections.SalesDB.ReleaseConnection(rawConnection);
     */
    #endregion

    #region Help:  Firing Integration Services Events
    /* This script component can fire events.
     *
     * Example of firing an error event:
     *  ComponentMetaData.FireError(10, "Process Values", "Bad value", "", 0, out cancel);
     *
     * Example of firing an information event:
     *  ComponentMetaData.FireInformation(10, "Process Values", "Processing has started", "", 0, fireAgain);
     *
     * Example of firing a warning event:
     *  ComponentMetaData.FireWarning(10, "Process Values", "No rows were received", "", 0);
     */
    #endregion

    public class Data
    {
        public Int64 delivery_id { get; set; }
        public String delivery_operator { get; set; }
        public DateTime? delivery_date { get; set; }
        public Decimal payment_amount { get; set; }
    }

    public class Data_error
    {
        public String file_name { get; set; }
        public String row { get; set; }
        public String description { get; set; }
    }

    public List<Data> list_data = new List<Data>();
    public List<Data_error> list_data_error = new List<Data_error>();

    /// <summary>
    /// This method is called once, before rows begin to be processed in the data flow.
    ///
    /// You can remove this method if you don't need to do anything here.
    /// </summary>
    public override void PreExecute()
    {
        base.PreExecute();

        Thread.CurrentThread.CurrentCulture = new CultureInfo("ru-RU", false);

        foreach (string file_type in new string[]{"boxberry", "others"})
        {

            /*
            Delivery_date(A)
            Delivery_id(B)
            Amount(D)
            */
            if (file_type == "boxberry")
            foreach (string file_path in Directory.GetFiles(String.Format(@"\\mir-sdb-005\BI Data\Input\delivery_payment\data\{0}\", file_type), "*.csv"))
            {
                string file_name = Path.GetFileName(file_path);
                foreach (string row in File.ReadAllLines(file_path, Encoding.GetEncoding("Windows-1251")).Skip(1))
                {
                    string[] columns = row.Split(';');
                    Data data = new Data();

                    try
                    {

                        Int64 delivery_id;
                        if (Int64.TryParse(columns[1], out delivery_id))
                            data.delivery_id = delivery_id;
                        else
                        {
                            list_data_error.Add
                            (
                                new Data_error()
                                {
                                    file_name = file_name,
                                    row = row,
                                    description = String.Format("Attempted conversion of '{0}' failed.", columns[1])
                                }
                            );
                            continue;
                        }

                        // Regex.Replace(columns[3], @"\s", "")
                        decimal payment_amount;
                        if (decimal.TryParse(columns[3], out payment_amount))
                            data.payment_amount = payment_amount;
                        else
                        {
                            list_data_error.Add
                            (
                                new Data_error()
                                {
                                    file_name = file_name,
                                    row = row,
                                    description = String.Format("Attempted conversion of '{0}' failed.", columns[3])
                                }
                            );
                            continue;
                        }

                        DateTime delivery_date;
                        if (DateTime.TryParse(columns[0], out delivery_date))
                            data.delivery_date = delivery_date;

                        data.delivery_operator = "БОКСБЕРРИ РУ ООО";

                        list_data.Add(data);
                    }
                    catch (IndexOutOfRangeException exception)
                    {
                        list_data_error.Add(new Data_error() { file_name = file_name, row = row, description = exception.Message.ToString() });
                    }
                }

                if (list_data_error.Where(i => i.file_name == file_name).Count() == 0)
                {
                    File.Copy(file_path, string.Format(@"\\mir-sdb-005\BI Data\Input\delivery_payment\archive\{0}\{1}", file_type, file_name), true);
                    File.Delete(file_path);
                }
            }

            /*
            delivery_operator(A)
            Delivery_id(B)
            Delivery_date(C)
            amount(D)
            */
            if (file_type == "others")
            foreach (string file_path in Directory.GetFiles(String.Format(@"\\mir-sdb-005\BI Data\Input\delivery_payment\data\{0}\", file_type), "*.csv"))
            {
                string file_name = Path.GetFileName(file_path);
                foreach (string row in File.ReadAllLines(file_path, Encoding.GetEncoding("Windows-1251")).Skip(1))
                {
                    string[] columns = row.Split(';');
                    Data data = new Data();

                    try
                    {

                        Int64 delivery_id;
                        if (Int64.TryParse(columns[1], out delivery_id))
                            data.delivery_id = delivery_id;
                        else
                        {
                            list_data_error.Add
                            (
                                new Data_error()
                                {
                                    file_name = file_name,
                                    row = row,
                                    description = String.Format("Attempted conversion of '{0}' failed.", columns[1])
                                }
                            );
                            continue;
                        }

                        // Regex.Replace(columns[3], @"\s", "")
                        decimal payment_amount;
                        if (decimal.TryParse(columns[3], out payment_amount))
                            data.payment_amount = payment_amount;
                        else
                        {
                            list_data_error.Add
                            (
                                new Data_error()
                                {
                                    file_name = file_name,
                                    row = row,
                                    description = String.Format("Attempted conversion of '{0}' failed.", columns[3])
                                }
                            );
                            continue;
                        }

                        DateTime delivery_date;
                        if (DateTime.TryParse(columns[2], out delivery_date))
                            data.delivery_date = delivery_date;

                        data.delivery_operator = columns[0];

                        list_data.Add(data);
                    }
                    catch (IndexOutOfRangeException exception)
                    {
                        list_data_error.Add(new Data_error() { file_name = file_name, row = row, description = exception.Message.ToString() });
                    }
                }

                if (list_data_error.Where(i => i.file_name == file_name).Count() == 0)
                {
                    File.Copy(file_path, string.Format(@"\\mir-sdb-005\BI Data\Input\delivery_payment\archive\{0}\{1}", file_type, file_name), true);
                    File.Delete(file_path);
                }
            }
        }
    }
    /// <summary>
    /// This method is called after all the rows have passed through this component.
    ///
    /// You can delete this method if you don't need to do anything here.
    /// </summary>
    public override void PostExecute()
    {
        base.PostExecute();
        /*
         * Add your code here
         */
    }

    public override void CreateNewOutputRows()
    {
        /*
          Add rows by calling the AddRow method on the member variable named "<Output Name>Buffer".
          For example, call MyOutputBuffer.AddRow() if your output was named "MyOutput".
        */
        foreach (Data data in list_data)
        {
            OutputBuffer.AddRow();
            OutputBuffer.deliveryid = data.delivery_id;
            OutputBuffer.deliveryoperator = data.delivery_operator;

            if (data.delivery_date.HasValue)
                OutputBuffer.deliverydate = (DateTime)data.delivery_date;
            else
                OutputBuffer.deliverydate_IsNull = true;
            OutputBuffer.paymentamount = data.payment_amount;
        }


        foreach (Data_error data_error in list_data_error)
        {
            OutputErrorsBuffer.AddRow();
            OutputErrorsBuffer.filename = data_error.file_name;
            OutputErrorsBuffer.row = data_error.row;
            OutputErrorsBuffer.description = data_error.description;
        }
    }
}
```