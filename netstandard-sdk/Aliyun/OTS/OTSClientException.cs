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

using System.Net;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTS客户端错误，用来表示OTS SDK运行时遇到的错误。
    /// </summary>
    public class OTSClientException : OTSException
    {
        /// <summary>
        /// 错误信息。
        /// </summary>
        public string ErrorMessage;
        
        /// <summary>
        /// HTTP返回码（如果有）。
        /// </summary>
        public HttpStatusCode HttpStatusCode { get; private set; }

        public OTSClientException(string errorMessage)
            : base(errorMessage)
        {
            ErrorMessage = errorMessage;
        }

        public OTSClientException(string errorMessage, HttpStatusCode httpCode)
            : this(errorMessage)
        {
            HttpStatusCode = httpCode;
        }
    }
}
