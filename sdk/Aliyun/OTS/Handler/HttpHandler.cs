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
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;


namespace Aliyun.OTS.Handler
{
    public class HttpHandler : PipelineHandler
    {
        public override void HandleBefore(Context context)
        {
            HttpContent content = new ByteArrayContent(context.HttpRequestBody);
            content.Headers.Clear();

            foreach (var item in context.HttpRequestHeaders)
            {
                content.Headers.Add(item.Key, item.Value);
            }

            Task<HttpResponseMessage> task = context.HttpClient.PostAsync(context.APIName, content);
            context.HttpTask = task;
        }

        public override void HandleAfter(Context context)
        {
            HttpResponseMessage responseMessage = context.HttpTask.Result;
            var task = responseMessage.Content.ReadAsByteArrayAsync();
            task.Wait();

            if (OTSClientTestHelper.HTTPResponseBodyIsSet) {
                context.HttpResponseBody = OTSClientTestHelper.HTTPResponseBody;
            } else {
                context.HttpResponseBody = task.Result;
            }
            
            if (OTSClientTestHelper.HttpStatusCodeIsSet) {
                context.HttpResponseStatusCode = OTSClientTestHelper.HttpStatusCode;
            } else {
                context.HttpResponseStatusCode = responseMessage.StatusCode;
            }

            if (OTSClientTestHelper.HttpResponseHeadersIsSet) {
                context.HttpResponseHeaders = OTSClientTestHelper.HttpRequestHeaders;
            } else {
                context.HttpResponseHeaders = new Dictionary<string, string>();
                foreach (var item in responseMessage.Headers)
                {
                    context.HttpResponseHeaders.Add(item.Key.ToLower(), item.Value.ElementAt(0));
                }
            }
        }
    }
}
