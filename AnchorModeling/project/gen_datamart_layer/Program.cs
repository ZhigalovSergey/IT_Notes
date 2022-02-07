using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace gen_datamart_layer
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine(args[0]);
            string dir = args[0];

            string text;
            string fl_new;

            string fl_json = dir + "\\metadata.json";
            string json = File.ReadAllText(fl_json);

            JObject mt = JObject.Parse(json);

            JToken mapping = mt.SelectToken("$.mapping");
            Console.WriteLine("mapping is: " + mapping);
            Dictionary<string, string> dict_attr = JsonConvert.DeserializeObject<Dictionary<string, string>>(mapping.ToString());

            string[] bk = mt.SelectToken("$.raw_table.business_key").Select(s => (string)s).ToArray();
            Console.WriteLine("bk is : " + String.Join("; ", bk));

            string attr_bk = bk[0];

            string anchor = (string)mt.SelectToken("$.anchor");
            Console.WriteLine("anchor is : " + anchor);

            string src_name = (string)mt.SelectToken("$.src_name");
            Console.WriteLine("src_name is : " + src_name);
            Console.ReadLine();

            string tbl_path = dir + "\\test\\dbo\\tbl\\";
            if (!Directory.Exists(tbl_path))
            {
                Directory.CreateDirectory(tbl_path);
            }

            string proc_path = dir + "\\test\\dbo\\proc\\";
            if (!Directory.Exists(proc_path))
            {
                Directory.CreateDirectory(proc_path);
            }

            // create datamart.sql
            text = File.ReadAllText(dir + "\\template\\dbo\\tbl\\datamart.sql");
            fl_new = string.Format(dir + "\\test\\dbo\\tbl\\" + anchor + ".sql");
            text = text.Replace("#anchor#", anchor);
            string tbl_columns = File.ReadAllText(dir + "\\metadata\\metadata_tbl_columns.sql");
            string src_attr;
            string attr;
            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                src_attr = kvp.Key;
                attr = kvp.Value;

                tbl_columns = tbl_columns.Replace("#" + src_attr + "#", anchor + "_" + attr);
            }
            text = text.Replace("#attrs#", tbl_columns);
            File.WriteAllText(fl_new, text);

            // create datamart_sync.sql
            text = File.ReadAllText(dir + "\\template\\dbo\\proc\\datamart_sync.sql");
            fl_new = string.Format(dir + "\\test\\dbo\\proc\\" + anchor + "_sync.sql");
            text = text.Replace("#anchor#", anchor);

            string list = File.ReadAllText(dir + "\\metadata\\metadata_proc_columns_list.sql");
            string init = File.ReadAllText(dir + "\\metadata\\metadata_proc_columns_init.sql");
            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                src_attr = kvp.Key;
                attr = kvp.Value;

                list = list.Replace("#" + src_attr + "#", anchor + "_" + attr);
                init = init.Replace("#" + src_attr + "#", anchor + "_" + attr);
            }
            text = text.Replace("#list#", list);
            text = text.Replace("#init#", init);

            // update attribute_business_key
            string upd_bk = File.ReadAllText(dir + "\\template\\dbo\\proc\\update_business_key.sql");
            upd_bk = upd_bk.Replace("#anchor#", anchor);
            upd_bk = upd_bk.Replace("#src_name#", src_name);
            upd_bk = upd_bk.Replace("#attr_bk#", attr_bk);
            text = text.Replace("#upd_business_key#", upd_bk);

            // update attributes
            string upd_attrs = "";
            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                if (bk.Contains(kvp.Key)) continue;
                src_attr = kvp.Key;
                attr = kvp.Value;
                string upd_attr = File.ReadAllText(dir + "\\template\\dbo\\proc\\update_attr.sql");

                upd_attr = upd_attr.Replace("#anchor#", anchor);
                upd_attr = upd_attr.Replace("#attr#", attr);
                upd_attrs += upd_attr;
            }

            text = text.Replace("#upd_attrs#", upd_attrs);
            File.WriteAllText(fl_new, text);

            string list_for_ssis = "";
            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                if (bk.Contains(kvp.Key)) continue;
                list_for_ssis += "\"" + kvp.Value + "\", \r";
            }
            File.WriteAllText(dir + "\\list_for_ssis.txt", list_for_ssis);

            Console.ReadLine();
        }
    }
}
