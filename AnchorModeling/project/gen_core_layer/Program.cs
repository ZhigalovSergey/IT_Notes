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

            // type_obj: anchor, attribute_business_key, anchor_sync, attributes, all

            Console.WriteLine(args[0]);
            string dir = args[0];
            string type_obj = args[1];

            string fl_json = dir + "\\metadata.json";
            string json = File.ReadAllText(fl_json);

            string text;
            string anchor = "utm_extended";
            string fl_new;

            anchor anchor_obj = new anchor();

            if (type_obj == "anchor" || type_obj == "all")
            { 
                anchor_obj.gen_anchor(anchor, dir);
            }

            if (type_obj == "attribute_business_key" || type_obj == "all")
            {
                anchor_obj.gen_attribute_business_key(anchor, dir);
            }

            if (type_obj == "anchor_sync" || type_obj == "all")
            {
                string src_name = "gbq";
                anchor_obj.gen_anchor_sync(anchor, dir, src_name);
            }

            attributes attr_obj = new attributes();

            if (type_obj == "attributes" || type_obj == "all")
            {
                attr_obj.gen_attributes(dir);
            }
            Console.ReadLine();
        }
    }
}
