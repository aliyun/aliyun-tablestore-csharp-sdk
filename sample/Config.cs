using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aliyun.OTS;

namespace Aliyun.OTS.Samples
{
    internal class Config
    {
        public static string AccessKeyId = "<your access key id>";

        public static string AccessKeySecret = "<your access key secret>";

        public static string Endpoint = "<your endpoint>";

        public static string InstanceName = "<your instance name>";

        private static OTSClient OtsClient = null;

        public static OTSClient GetClient()
        {
            if (OtsClient != null)
            {
                return OtsClient;
            }

            OTSClientConfig config = new OTSClientConfig(Endpoint, AccessKeyId, AccessKeySecret, InstanceName);
            config.OTSDebugLogHandler = null;
            config.OTSErrorLogHandler = null;
            OtsClient = new OTSClient(config);
            return OtsClient;
        }
    }
}
