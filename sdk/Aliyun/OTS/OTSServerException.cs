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
using System.Net;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTS服务端错误类型，用来表示OTS返回的错误。
    /// <para>
    /// 完整的OTS出错信息请见<a href="http://docs.aliyun.com/ots/DeveloperGuide/ots-api">阿里云官网关于OTS错误信息的定义</a>。
    /// </para>
    /// </summary>
    public class OTSServerException : OTSException
    {
        /// <summary>
        /// API名称，例如 "/GetRow"，"/CeateTable"。
        /// </summary>
        public string APIName { get; private set; }

        /// <summary>
        /// HTTP返回码。 
        /// </summary>
        public HttpStatusCode HttpStatusCode { get; private set; }

        /// <summary>
        /// 错误类型字符串。
        /// </summary>
        public string ErrorCode { get; private set; }

        /// <summary>
        /// 错误消息字符串。
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// 本次请求的ID，用于OTS工程师定位错误使用。
        /// </summary>
        public string RequestID { get; private set; }

        private static string GetMessageString(string apiName, HttpStatusCode httpStatusCode, string errorCode, string errorMessage, string requestID)
        {
            string ret = String.Format(
                "OTS request failed, API: {0}, HTTPStatus: {1} {2}",
                apiName, (int)httpStatusCode, httpStatusCode
            );

            if (errorCode != null)
            {
                ret += String.Format(", ErrorCode: {0}", errorCode);
            }

            if (errorMessage != null)
            {
                ret += String.Format(", ErrorMessage: {0}", errorMessage);
            }

            if (requestID != null)
            {
                ret += String.Format(", Request ID: {0}", requestID);
            }

            return ret;
        }

        public OTSServerException(string apiName, HttpStatusCode httpStatusCode)
            : base(GetMessageString(apiName, httpStatusCode, null, null, null))
        {
            APIName = apiName;
            HttpStatusCode = httpStatusCode;
        }

        public OTSServerException(string apiName, HttpStatusCode httpStatusCode, string errorCode, string errorMessage)
            : base(GetMessageString(apiName, httpStatusCode, errorCode, errorMessage, null))
        {
            APIName = apiName;
            HttpStatusCode = httpStatusCode;
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
        }

        public OTSServerException(string apiName, HttpStatusCode httpStatusCode, string errorCode, string errorMessage, string requestID)
            : base(GetMessageString(apiName, httpStatusCode, errorCode, errorMessage, requestID))
        {
            APIName = apiName;
            HttpStatusCode = httpStatusCode;
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
            RequestID = requestID;
        }
    }
}
