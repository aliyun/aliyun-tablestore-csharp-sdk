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

using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Aliyun.OTS.Request;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.Handler
{
    public class Context
    {
        public OTSClientConfig ClientConfig { get; set; }
        
        public HttpClient HttpClient { get; set; }
        public string APIName { get; set; }
        public Task<HttpResponseMessage> HttpTask;
        
        public int RetryTimes { get; set; }
        
        // For Request
        public OTSRequest OTSRequest { get; set; }
        public string HttpRequestQuery { get; set; }
        public Dictionary<string, string> HttpRequestHeaders { get; set; }
        public byte[] HttpRequestBody { get; set; }

        // For Response
        public OTSResponse OTSReponse { get; set; }
        public HttpResponseMessage HttpResponseMessage { get; set; }
        public HttpStatusCode HttpResponseStatusCode { get; set; }
        public Dictionary<string, string> HttpResponseHeaders { get; set; }
        public byte[] HttpResponseBody { get; set; }

        public Context() 
        {
            RetryTimes = 0;
        }
    }
}
