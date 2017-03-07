/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using Aliyun.OTS.Aliyun.OTS.Util;
using Aliyun.OTS.Retry;
using System;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTS客户端配置，用来设置用户的权限信息等。
    /// </summary>
    public class OTSClientConfig
    {
        private static int DefaultConnectionLimit = 300;
        private static readonly string DefaultAPIVersion = "2014-08-08";
        private const string UserAgentPrefix = "aliyun-tablestore-sdk-dotnet/";
        private static readonly string _userAgent = GetDefaultUserAgent();

        /// <summary>
        /// OTS服务的地址（例如 'http://instance.cn-hangzhou.ots.aliyun.com:80'），必须以'http://'开头。
        /// </summary>
        public string EndPoint { get; set; }
        
        /// <summary>
        /// OTS的Access Key ID，通过官方网站申请。
        /// </summary>
        public string AccessKeyID { get; set; }
        
        /// <summary>
        /// OTS的Access Key Secret，通过官方网站申请。
        /// </summary>
        public string AccessKeySecret { get; set; }
        
        /// <summary>
        /// OTS实例名，通过官方网站控制台创建。
        /// </summary>
        public string InstanceName { get; set; }
        
        /// <summary>
        /// OTS协议的版本，默认为"2014-08-08"。无需改动。
        /// </summary>
        public string APIVersion { get; set; }
        
        /// <summary>
        /// 连接池的最大连接数，默认为300。
        /// </summary>
        public int ConnectionLimit { get; set; }
        
        /// <summary>
        /// 重试策略，默认为<see cref="DefaultRetryPolicy"/>。
        /// </summary>
        public RetryPolicy RetryPolicy { get; set; }

        public string UserAgent
        {
            get { return _userAgent; }
        }

        /// <summary>
        /// OTSClient的Log回调函数类型。如果要改变OTSClient默认的日志行为，需要定义这个类型的函数并
        /// 设置到OTSClientConfig中。
        /// </summary>
        /// <param name="message">回调函数被传入的日志信息。</param>
        public delegate void OTSLogHandler(string message);
        
        /// <summary>
        /// 错误日志的处理函数。默认行为是将日志输出到标准输出文件。
        /// </summary>
        public OTSLogHandler OTSErrorLogHandler { get; set; }
        
        /// <summary>
        /// 调试日志的处理函数。默认行为是将日志输出到标准输出文件。
        /// </summary>
        public OTSLogHandler OTSDebugLogHandler { get; set; }

        /// <summary>
        /// 跳过返回校验
        /// </summary>
        public bool SkipResponseValidation { get; set; }

        /// <summary>
        /// OTSClientConfig的构造函数。
        /// </summary>
        /// <param name="endPoint">OTS服务的地址</param>
        /// <param name="accessKeyID">OTS的Access Key ID</param>
        /// <param name="accessKeySecret">OTS的Access Key Secret</param>
        /// <param name="instanceName">OTS实例名</param>
        public OTSClientConfig(string endPoint, string accessKeyID, string accessKeySecret, string instanceName)
        {
            if (string.IsNullOrEmpty(endPoint))
                throw new ArgumentNullException("endPoint");

            if (string.IsNullOrEmpty(accessKeyID))
                throw new ArgumentNullException("accessKeyID");

            if (string.IsNullOrEmpty(accessKeySecret))
                throw new ArgumentNullException("AccessKeySecret");

            if (string.IsNullOrEmpty(instanceName))
                throw new ArgumentNullException("instanceName");

            EndPoint = endPoint.Trim();
            AccessKeyID = accessKeyID.Trim();
            AccessKeySecret = accessKeySecret.Trim();
            InstanceName = instanceName.Trim();
            ConnectionLimit = DefaultConnectionLimit;
            APIVersion = DefaultAPIVersion;
            RetryPolicy = RetryPolicy.DefaultRetryPolicy;
           
            OTSErrorLogHandler = OTSDefaultLogHandler.DefaultErrorLogHandler;
            OTSDebugLogHandler = OTSDefaultLogHandler.DefaultDebugLogHandler;
        }

        /// <summary>
        /// 获取User-Agent信息。
        /// </summary>
        private static string GetDefaultUserAgent()
        {
            return UserAgentPrefix +
                typeof(OTSClientConfig).Assembly.GetName().Version + "(" +
                OtsUtils.DetermineOsVersion() + "/" +
                Environment.OSVersion.Version + "/" +
                OtsUtils.DetermineSystemArchitecture() + ";" +
                Environment.Version + ")";
        }
    }
}
