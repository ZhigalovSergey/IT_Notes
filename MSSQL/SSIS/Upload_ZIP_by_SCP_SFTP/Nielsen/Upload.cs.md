```c#
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

class Upload
{
    public void Upload_to_Nielson(PrivateKeyFile keyFile, string LocalPath, string RemotePath)
    {
        try
        {
            string host = "eumft.nielseniq.com";
            string username = "RU_Goods";
            //string password = Dts.Variables["gfk_pass"].GetSensitiveValue().ToString();

            var keyFiles = new[] { keyFile };
            var methods = new List<AuthenticationMethod>
                {
                    //new PasswordAuthenticationMethod(username, password),
                    new PrivateKeyAuthenticationMethod(username, keyFiles)
                };

            Renci.SshNet.ConnectionInfo con = new Renci.SshNet.ConnectionInfo(host, 22, username, methods.ToArray());
            using (var client = new SftpClient(con))
            {
                client.Connect();

                string[] files = Directory.GetFiles(LocalPath, "*.zip", SearchOption.TopDirectoryOnly);

                foreach (string file in files)
                {
                    string fileName = file.Substring(LocalPath.Length);
                    var fileStream = new FileStream(file, FileMode.Open);
                    if (fileStream != null)
                    {
                        client.UploadFile(fileStream, String.Format("{0}{1}", RemotePath, fileName), null);
                    }
                    fileStream.Close();
                }

                client.Disconnect();
            }
        }
        catch (Exception e)
        {
            throw e;
        }
    }
}
```