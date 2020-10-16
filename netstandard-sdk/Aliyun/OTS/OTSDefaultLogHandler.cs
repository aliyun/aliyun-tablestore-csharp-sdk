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
using System;

namespace Aliyun.OTS
{
    /// <summary>
    /// 默认的日志处理，行为是将错误日志和调试日志打印到标准输出文件。
    /// </summary>
    public class OTSDefaultLogHandler
    {
        protected static string GetDateTimeString()
        {
            return DateTime.Now.ToString("o");
        }
        
        public static void DefaultErrorLogHandler(string message)
        {
            var dateString = GetDateTimeString();
            System.Console.WriteLine("OTSClient ERROR {0} {1}", dateString, message);
        }
        
        public static void DefaultDebugLogHandler(string message)
        {
            var dateString = GetDateTimeString();
            System.Console.WriteLine("OTSClient DEBUG {0} {1}", dateString, message);
        }
    }
}
