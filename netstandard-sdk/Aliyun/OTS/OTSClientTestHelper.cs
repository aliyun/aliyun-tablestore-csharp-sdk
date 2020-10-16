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
using System.Collections.Generic;

namespace Aliyun.OTS
{
    public static class OTSClientTestHelper
    {
        public static bool HTTPResponseBodyIsSet { get; private set; }
        public static byte[] HTTPResponseBody { get; private set; }
        
        public static bool HttpStatusCodeIsSet { get; private set; }
        public static HttpStatusCode HttpStatusCode { get; private set; }
        
        public static bool HttpResponseHeadersIsSet { get; private set; }
        public static Dictionary<string, string> HttpRequestHeaders { get; private set; }
        
        public static bool RetryTimesAndBackOffRecordSwith { get; private set; }
        public static int RetryTimes;
        public static List<int> RetryDelays { get; private set; }
        public static List<OTSServerException> RetryExceptions { get; private set; }
        
        public static void Reset()
        {
            HTTPResponseBodyIsSet = false;
            HttpStatusCodeIsSet = false;
            HttpResponseHeadersIsSet = false;
            RetryTimesAndBackOffRecordSwith = false;
        }
        
        public static void SetHTTPResponseBody(byte[] body)
        {
            HTTPResponseBodyIsSet = true;
            HTTPResponseBody = body;
        }
        
        public static void SetHttpStatusCode(HttpStatusCode code)
        {
            HttpStatusCodeIsSet = true;
            HttpStatusCode = code;
        }
        
        public static void SetHttpRequestHeaders(Dictionary<string, string> headers)
        {
            HttpResponseHeadersIsSet = true;
            HttpRequestHeaders = headers;
        }
        
        public static void TurnOnRetryTimesAndBackOffRecording()
        {
            RetryTimesAndBackOffRecordSwith = true;
            RetryTimes = 0;
            RetryDelays = new List<int>();
            RetryExceptions = new List<OTSServerException>();
        }
    }
}
