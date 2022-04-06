using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gen_core_layer
{
    class anchor
    {
        string text;
        string fl_new;

        public void gen_anchor(string anchor, string dir)
        {

            // create anchor
            text = File.ReadAllText(dir + "\\template\\core\\tbl\\anchor.sql");
            fl_new = string.Format(dir + "\\test\\core\\tbl\\" + anchor + ".sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_source.sql
            text = File.ReadAllText(dir + "\\template\\core\\tbl\\anchor_source_ref.sql");
            fl_new = string.Format(dir + "\\test\\core\\tbl\\" + anchor + "_source_ref.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);

            // anchor_sequence.sql
            text = File.ReadAllText(dir + "\\template\\core\\seq\\anchor_sequence.sql");
            fl_new = string.Format(dir + "\\test\\core\\seq\\" + anchor + "_sequence.sql");
            text = text.Replace("#anchor#", anchor);
            File.WriteAllText(fl_new, text);
        }

        public void gen_attribute_business_key(string anchor, string dir, string src_name, string attr_bk_1)
        {

            // attribute_business_key.sql
            text = File.ReadAllText(dir + "\\template\\core\\tbl\\attribute_business_key.sql");
            fl_new = string.Format(dir + "\\test\\core\\tbl\\" + anchor + "_s_" + src_name + ".sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            text = text.Replace("#attr_bk_1#", attr_bk_1);
            File.WriteAllText(fl_new, text);

            // anchor_sync.sql
            text = File.ReadAllText(dir + "\\template\\core\\proc\\anchor_sync.sql");
            fl_new = string.Format(dir + "\\test\\core\\proc\\" + anchor + "_sync.sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            File.WriteAllText(fl_new, text);
        }

        public void gen_anchor_sync(string anchor, string dir, string src_name)
        {
            // anchor_sync.sql
            text = File.ReadAllText(dir + "\\template\\core\\proc\\anchor_sync.sql");
            fl_new = string.Format(dir + "\\test\\core\\proc\\" + anchor + "_sync.sql");
            text = text.Replace("#anchor#", anchor);
            text = text.Replace("#src_name#", src_name);
            File.WriteAllText(fl_new, text);
        }
    }
}
