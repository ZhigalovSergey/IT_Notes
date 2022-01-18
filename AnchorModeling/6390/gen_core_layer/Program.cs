using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace gen_core_layer
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine(args[0]);
            string dir = args[0];

            string text;
            string anchor = "utm_extended";
            string fl_new;

            // create anchor
            text = File.ReadAllText(dir + "\\template\\tbl\\anchor.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + ".sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_source.sql
            text = File.ReadAllText(dir + "\\template\\tbl\\anchor_source_ref.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_source_ref.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_sequence.sql
            text = File.ReadAllText(dir + "\\template\\seq\\anchor_sequence.sql");
            fl_new = string.Format(dir + "\\test\\seq\\" + anchor + "_sequence.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            string src_name = "gbq";
            // "id", "source", "medium", "campaign", "content", "term"
            string attr_bk_1 = "hash";
            string attr_bk_2 = "source";
            string attr_bk_3 = "medium";
            string attr_bk_4 = "campaign";
            string attr_bk_5 = "content";
            string attr_bk_6 = "term";

            // attribute_business_key.sql
            text = File.ReadAllText(dir + "\\template\\tbl\\attribute_business_key.sql");
            fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_s_" + src_name + ".sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            text = text.Replace("#attr_bk_1#", attr_bk_1);
            text = text.Replace("#attr_bk_2#", attr_bk_2);
            text = text.Replace("#attr_bk_3#", attr_bk_3);
            text = text.Replace("#attr_bk_4#", attr_bk_4);
            text = text.Replace("#attr_bk_5#", attr_bk_5);
            text = text.Replace("#attr_bk_6#", attr_bk_6);
            File.WriteAllText(fl_new, text);

            // anchor_sync.sql
            text = File.ReadAllText(dir + "\\template\\proc\\anchor_sync.sql");
            fl_new = string.Format(dir + "\\test\\proc\\" + anchor + "_sync.sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            File.WriteAllText(fl_new, text);

            //string fl_list = dir + "\\list.txt";
            //StreamReader reading = File.OpenText(fl_list);

            string fl_json = dir + "\\metadata.json";
            string json = File.ReadAllText(fl_json);

            JObject mt = JObject.Parse(json);
            JToken mapping = mt.SelectToken("$.mapping");
            string[] bk = mt.SelectToken("$.raw_table.business_key").Select(s => (string)s).ToArray();
            Console.WriteLine(mapping);
            Console.ReadLine();
            Dictionary<string, string> dict_attr = JsonConvert.DeserializeObject<Dictionary<string, string>>(mapping.ToString());
            string src_attr;
            string attr;

            foreach (KeyValuePair<string, string> kvp in dict_attr)
            {
                if (bk.Contains(kvp.Key)) continue;
                Console.WriteLine("src_attr = {0}, attr = {1}", kvp.Key, kvp.Value);
                src_attr = kvp.Key;
                attr = kvp.Value;

                text = File.ReadAllText(dir + "\\template\\tbl\\attribute.sql");
                fl_new = string.Format(dir + "\\test\\tbl\\" + anchor + "_x_" + attr + ".sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#src_name#", src_name);
                File.WriteAllText(fl_new, text);

                text = File.ReadAllText(dir + "\\template\\proc\\attribute_sync.sql");
                fl_new = string.Format(dir + "\\test\\proc\\" + anchor + "_x_" + attr + "_sync.sql");
                text = text.Replace("#anchor#", anchor);
                text = text.Replace("#attr#", attr);
                text = text.Replace("#src_name#", src_name);
                text = text.Replace("#src_attr#", src_attr);
                File.WriteAllText(fl_new, text);
            }

            Console.ReadLine();
        }
    }
}
