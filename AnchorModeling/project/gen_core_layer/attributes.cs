using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gen_core_layer
{
    class attributes
    {
        string text;
        string fl_new;

        public void gen_attributes(string dir)
        {
            string fl_json = dir + "\\metadata.json";
            string json = File.ReadAllText(fl_json);

            JObject mt = JObject.Parse(json);

            JToken mapping = mt.SelectToken("$.mapping");
            Console.WriteLine("mapping is: " + mapping);
            Dictionary<string, string> dict_attr = JsonConvert.DeserializeObject<Dictionary<string, string>>(mapping.ToString());

            JToken columns = mt.SelectToken("$.raw_table.columns");
            Console.WriteLine("columns is: " + columns);
            Dictionary<string, string> dict_columns = JsonConvert.DeserializeObject<Dictionary<string, string>>(columns.ToString());

            string[] bk = mt.SelectToken("$.raw_table.business_key").Select(s => (string)s).ToArray();
            Console.WriteLine("bk is : " + String.Join("; ", bk));

            string bk1 = bk[0];

            string schema = (string)mt.SelectToken("$.raw_table.schema");
            Console.WriteLine("schema is : " + schema);

            string table = (string)mt.SelectToken("$.raw_table.table");
            Console.WriteLine("table is : " + table);

            string anchor = (string)mt.SelectToken("$.anchor");
            Console.WriteLine("anchor is : " + anchor);

            string src_name = (string)mt.SelectToken("$.src_name");
            Console.WriteLine("src_name is : " + src_name);
            Console.ReadLine();


            string tbl_path = dir + "\\test\\core\\tbl\\";
            if (!Directory.Exists(tbl_path))
            {
                Directory.CreateDirectory(tbl_path);
            }

            string proc_path = dir + "\\test\\core\\proc\\";
            if (!Directory.Exists(proc_path))
            {
                Directory.CreateDirectory(proc_path);
            }

            string src_attr;
            string attr;
            string src_type;

            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                if (bk.Contains(kvp.Key)) continue;
                Console.WriteLine("src_attr = {0}, attr = {1}", kvp.Key, kvp.Value);
                src_attr = kvp.Key;
                attr = kvp.Value;
                src_type = dict_columns[src_attr];

                text = File.ReadAllText(dir + "\\template\\core\\tbl\\attribute.sql");
                fl_new = string.Format(dir + "\\test\\core\\tbl\\" + anchor + "_x_" + attr + ".sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#type#", src_type);
                text = text.Replace("#src_name#", src_name);
                File.WriteAllText(fl_new, text);

                text = File.ReadAllText(dir + "\\template\\core\\proc\\attribute_sync.sql");
                fl_new = string.Format(dir + "\\test\\core\\proc\\" + anchor + "_x_" + attr + "_sync.sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#type#", src_type);
                text = text.Replace("#src_name#", src_name);
                text = text.Replace("#src_attr#", src_attr);
                text = text.Replace("#schema#", schema);
                text = text.Replace("#table#", table);

                text = text.Replace("#business_key#", bk1);
                File.WriteAllText(fl_new, text);
            }

        }
    }
}
