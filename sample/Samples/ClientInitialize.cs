using System;
using Aliyun.OTS.Request;

namespace Aliyun.OTS.Samples
{
    public static class ClientInitialize
    {

        public static void ErrorLog(string message)
        {
            Console.WriteLine(message);
        }

        public static void InitializeClient()
        {
            OTSClientConfig config = new OTSClientConfig(Config.Endpoint, Config.AccessKeyId, Config.AccessKeySecret, Config.InstanceName);
            config.AccessKeyID = Config.AccessKeyId; // 访问OTS服务所需的云账号的accessid
            config.AccessKeySecret = Config.AccessKeySecret; // 访问OTS服务所需的云账号的accesskey
            config.InstanceName = Config.InstanceName; // OTS的实例名
            config.EndPoint = Config.Endpoint; // OTS的服务访问地址
            config.ConnectionLimit = 300; // Client内部的连接池的连接数上限
            config.OTSDebugLogHandler = null; // 将DebugLogHandler设置为null，可关闭client内部的debug log的输出，否则默认会输出到标准输出
            config.OTSErrorLogHandler = ErrorLog; // 也可自定义LogHandler，将日志输出进行定制

            OTSClient client = new OTSClient(config); // 初始化ots client

            ListTableRequest request = new ListTableRequest();
            client.ListTable(request);
        }
    }
}
