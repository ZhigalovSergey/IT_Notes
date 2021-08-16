```c#
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RawMigrator
{
    class Program
    {
        private static string sourceRoot = @"C:\Users\zhigalov\Desktop\GOODS\GitLab\DWH\Databases\MDWH\";
        public static List<string> listObjType = new List<string>();
        static void Main(string[] args)
        {
            try
            {
                string[] folderNames = Directory.GetDirectories(sourceRoot, "raw*", SearchOption.TopDirectoryOnly);
                Console.WriteLine("The number of directories starting with raw is {0}.", folderNames.Length);
                int prefixLen = sourceRoot.Length;
                string schemaName;
                foreach (string fln in folderNames)
                {
                    schemaName = fln.Substring(prefixLen);
                    Console.WriteLine(String.Format("    {0}/", schemaName));
                    ProcessDirectory(fln, 1);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The process failed: {0}", e.ToString());
            }
            Console.ReadLine();

            List<string> distinct = listObjType.Distinct().ToList();
            Console.WriteLine("listObjType:");
            foreach (string value in distinct)
            {
                Console.WriteLine("    {0}", value);
            }
            Console.ReadLine();
        }

        // Process all files in the directory passed in, recurse on any directories
        // that are found, and process the files they contain.
        public static void ProcessDirectory(string targetDirectory, int lvl)
        {
            lvl = lvl + 1;

            // Process the list of files found in the directory.
            string[] fileEntries = Directory.GetFiles(targetDirectory);
            foreach (string fileName in fileEntries)
                ProcessFile(fileName, lvl);

            // Recurse into subdirectories of this directory.
            string[] subdirectoryEntries = Directory.GetDirectories(targetDirectory);
            int prefixLen = targetDirectory.Length + 1;
            foreach (string subdirectory in subdirectoryEntries)
            {
                DirectoryInfo di = new DirectoryInfo(subdirectory);
                listObjType.Add(di.Name);
                Console.WriteLine(String.Format(string.Concat(Enumerable.Repeat("    ", lvl)) + "{0}/", subdirectory.Substring(prefixLen)));
                ProcessDirectory(subdirectory, lvl);
            }
        }

        public static void ProcessFile(string path, int lvl)
        {
            FileInfo fi = new FileInfo(path);
            Console.WriteLine(string.Concat(Enumerable.Repeat("    ", lvl)) + "{0}", fi.Name);
        }
    }
}
```