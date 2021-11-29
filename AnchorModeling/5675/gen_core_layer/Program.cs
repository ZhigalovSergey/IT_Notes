using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gen_core_layer
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine(args[0]);

            // tbl\utm_tmp.sql
            string dir_tmp = args[0];

            string fl_list = dir_tmp + "list.txt";
            StreamReader reading = File.OpenText(fl_list);

            string fl_update = dir_tmp + "update_template.sql";
            string fl_proc = dir_tmp + "utm.sql";

            string text;
            string str;
            string fl_new;
            while ((str = reading.ReadLine()) != null)
            {
                text = File.ReadAllText(dir_tmp + "template\\tbl\\utm_tmp.sql");
                fl_new = string.Format(dir_tmp + "test\\tbl\\utm_" + str + ".sql");
                text = text.Replace("###", str);
                File.WriteAllText(fl_new, text);

                text = File.ReadAllText(dir_tmp + "template\\proc\\utm_tmp_sync.sql");
                fl_new = string.Format(dir_tmp + "test\\proc\\utm_" + str + "_sync.sql");
                text = text.Replace("###", str);
                File.WriteAllText(fl_new, text);

                text = File.ReadAllText(fl_update);
                text = text.Replace("###", str);
                File.AppendAllText(fl_proc, text);
            }
    
            Console.ReadLine();
        }
    }
}
