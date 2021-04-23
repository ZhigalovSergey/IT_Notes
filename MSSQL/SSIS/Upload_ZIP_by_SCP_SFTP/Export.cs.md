```c#
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


class Export
{
    public void Export_to_flat_file(string Connection, string Query, string FileFullPath, string FileDelimite)
    { 
        //Read data from SQL SERVER
        using (OleDbConnection connection = new OleDbConnection(Connection))
        {
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string LogFolder = @".\";

            try
            {
                OleDbCommand command = new OleDbCommand(Query, connection);
                connection.Open();
                OleDbDataReader reader = command.ExecuteReader();
            
                StreamWriter sw = null;
                sw = new StreamWriter(FileFullPath, false);

                // Write the Header Row to File
                int ColumnCount = reader.FieldCount;
                for (int ic = 0; ic<ColumnCount; ic++)
                {
                    sw.Write(reader.GetName(ic));
                    if (ic<ColumnCount - 1)
                    {
                        sw.Write(FileDelimite);
                    }
                }
                sw.Write(sw.NewLine);


                // Write All Rows to the File
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        for (int ir = 0; ir<ColumnCount; ir++)
                        {
                            if (!reader.IsDBNull(ir))
                            {
                                sw.Write(reader.GetString(ir));
                            }
                            if (ir<ColumnCount - 1)
                            {
                                sw.Write(FileDelimite);
                            }
                        }
                        sw.Write(sw.NewLine);
                    }
                }
                sw.Close();

                reader.Close();
            }
            catch (Exception exception)
            {
                // Create Log File for Errors
                using (StreamWriter sw = File.CreateText(LogFolder
                    + "\\" + "ErrorLog_" + datetime + ".log"))
                {
                    sw.WriteLine(exception.ToString());
                }

            }
        }
    }
}
```