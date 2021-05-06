using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class Export
{
    public void Export_to_flat_file(string Connection, string Query, string FileFullPath, string FileDelimite, Int32 TimeOut)
    {
        try
        {
            //Read data from SQL SERVER
            using (OleDbConnection connection = new OleDbConnection(Connection))
            {
                Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US", false);
                OleDbCommand command = new OleDbCommand(Query, connection);
                connection.Open();
                command.CommandTimeout = TimeOut;
                OleDbDataReader reader = command.ExecuteReader();

                StreamWriter sw = null;
                sw = new StreamWriter(FileFullPath, false);

                // Write the Header Row to File
                int ColumnCount = reader.FieldCount;
                for (int ic = 0; ic < ColumnCount; ic++)
                {
                    sw.Write(reader.GetName(ic));
                    if (ic < ColumnCount - 1)
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
                        for (int ir = 0; ir < ColumnCount; ir++)
                        {
                            if (!reader.IsDBNull(ir))
                            {
                                sw.Write(reader.GetValue(ir).ToString());
                            }
                            if (ir < ColumnCount - 1)
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

        }
        catch (Exception e)
        {
            throw e;
        }
    }
}