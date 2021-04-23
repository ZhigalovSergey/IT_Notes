```c#
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gfk
{
    class Program
    {

        static void Main(string[] args)
        {
            string datetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string LogFolder = @".\";

            //Declare Variables and provide values
            string FileNamePart = "GOODS_ADDRESS"+"_"+datetime;  //Datetime will be added to it
            string DestinationFolder = @".\";
            string FileDelimiter = "\t";        //You can provide comma or pipe or whatever you like
            string FileExtension = ".txt";      //Provide the extension you like such as .txt or .csv

            //Create Connection to SQL Server in which you like to load files
            string MDWHConnection = "Data Source=dwh.prod.lan;Initial Catalog=MDWH;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False";
            string queryString = "exec interface.gfk_cities";

            //Read data from SQL SERVER

            Export ex = new Export();
            ex.Export_to_flat_file(MDWHConnection, queryString, DestinationFolder+FileNamePart+FileExtension, FileDelimiter);
            
            //Console.ReadLine();
        }

    }
}
```