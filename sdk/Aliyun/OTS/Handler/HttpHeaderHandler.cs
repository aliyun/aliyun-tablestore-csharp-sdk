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
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;

namespace Aliyun.OTS.Handler
{
    public class HttpHeaderHandler : PipelineHandler
    {
        public static int MAX_TIME_DEVIATION_IN_MINUTES = 15;

        private static string[] HeaderNames = new string[]{
            "x-ots-contentmd5",
            "x-ots-requestid",
            "x-ots-date",
            "x-ots-contenttype",
        };

        public HttpHeaderHandler(PipelineHandler innerHandler) : base(innerHandler) { }

        private string MakeHeaderString(Dictionary<string, string> headers)
        {
            List<string> items = new List<string>();

            foreach (var item in headers)
            {
                if (item.Key.StartsWith("x-ots-"))
                {
                    items.Add(String.Format("{0}:{1}", item.Key, item.Value));
                }
            }

            items.Sort();
            return String.Join("\n", items);
        }

        private string ComputeSignature(string signatureString, string accessKeySecret)
        {
            var hmac = new HMACSHA1(System.Text.Encoding.ASCII.GetBytes(accessKeySecret));
            byte[] hashValue = hmac.ComputeHash(System.Text.Encoding.ASCII.GetBytes(signatureString));
            string signature = System.Convert.ToBase64String(hashValue);
            return signature;
        }

        private string MakeRequestSignature(Context context, Dictionary<string, string> headers)
        {
            string headerString = MakeHeaderString(headers);
            string signatureString = context.APIName + "\nPOST\n\n" + headerString + '\n';
            return ComputeSignature(signatureString, context.ClientConfig.AccessKeySecret);
        }

        private string MakeResponseSignature(Context context)
        {
            string headerString = MakeHeaderString(context.HttpResponseHeaders);
            string signatureString = headerString + "\n" + context.APIName;
            return ComputeSignature(signatureString, context.ClientConfig.AccessKeySecret);
        }

        public override void HandleBefore(Context context)
        {
            
            var headers = new Dictionary<string, string>();

            // Step 1, compute Content MD5
            var md5hash = MD5.Create();
            byte[] hashData = md5hash.ComputeHash(context.HttpRequestBody);
            string contentMD5 = Convert.ToBase64String(hashData);
            headers.Add("x-ots-contentmd5", contentMD5);

            // Step 2, make date time string
            var dateString = DateTime.UtcNow.ToString("R");
            headers.Add("x-ots-date", dateString);

            // Step 3, other headers
            headers.Add("x-ots-apiversion", context.ClientConfig.APIVersion);
            headers.Add("x-ots-accesskeyid", context.ClientConfig.AccessKeyID);
            headers.Add("x-ots-instancename", context.ClientConfig.InstanceName);

            headers.Add("x-ots-user-agent", context.ClientConfig.UserAgent);

            // Step 4, compute signature
            string signature = MakeRequestSignature(context, headers);
            headers.Add("x-ots-signature", signature);

            context.HttpRequestHeaders = headers;
            
            InnerHandler.HandleBefore(context);
        }

        private void CheckOtherHeaders(Context context)
        {
            var headers = context.HttpResponseHeaders;

            // Step 1, make sure we have all headers
            if ((int)context.HttpResponseStatusCode >= 200
                && (int)context.HttpResponseStatusCode < 300)
            {
                foreach (string name in HeaderNames)
                {
                    if (!headers.ContainsKey(name))
                    {
                        throw new OTSClientException(String.Format(
                            "{0} is missing in response header.", name
                        ));
                    }
                }
            }

            // Step 2, check md5
            if (headers.ContainsKey("x-ots-contentmd5"))
            {
                var md5hash = MD5.Create();
                byte[] hashData = md5hash.ComputeHash(context.HttpResponseBody);
                string contentMD5 = System.Convert.ToBase64String(hashData);
                if (contentMD5 != headers["x-ots-contentmd5"])
                {
                    throw new OTSClientException("MD5 mismatch in response.");
                }
            }

            // Step 3, check date
            if (headers.ContainsKey("x-ots-date"))
            {
                string serverTimeString = headers["x-ots-date"];
                DateTime serverTime;
                try
                {
                    serverTime = DateTime.Parse(serverTimeString).ToUniversalTime();
                }
                catch (System.FormatException)
                {
                    throw new OTSClientException(String.Format(
                        "Invalid date format in response: {0}", serverTimeString
                    ));
                }

                var clientTime = DateTime.UtcNow;
                if (Math.Abs((serverTime - clientTime).TotalSeconds) > MAX_TIME_DEVIATION_IN_MINUTES * 60)
                {
                    throw new OTSClientException("The difference between date in response and system time is more than 15 minutes.");
                }
            }

        }

        private void CheckAuthorization(Context context)
        {
            // Step 1, Check if authorization header is there
            if (!context.HttpResponseHeaders.ContainsKey("authorization"))
            {
                if ((int)context.HttpResponseStatusCode >= 200
                    && (int)context.HttpResponseStatusCode < 300)
                {
                    throw new OTSClientException("\"Authorization\" is missing in response header.");
                }

                return;
            }

            string authorization = context.HttpResponseHeaders["authorization"];

            // Step 2, check if authorization is valid
            if (!authorization.StartsWith("OTS "))
            {
                throw new OTSClientException("Invalid Authorization in response.");
            }

            string[] splits = authorization.Substring(4).Split(':');
            if (splits.Length != 2)
            {
                throw new OTSClientException("Invalid Authorization in response.");
            }

            // Step 3, check accessKeyID
            string accessKeyID = splits[0];
            if (accessKeyID != context.ClientConfig.AccessKeyID)
            {
                throw new OTSClientException("Access Key ID mismatch in response.");
            }

            // Step 4, check signature
            string signature = splits[1];
            if (signature != MakeResponseSignature(context))
            {
                throw new OTSClientException("Signature mismatch in response.");
            }
        }

        public override void HandleAfter(Context context)
        {
            InnerHandler.HandleAfter(context);

            // Disable reponse validation
            if (context.ClientConfig.SkipResponseValidation)
            {
                return;
            }

            try
            {
                CheckOtherHeaders(context);

                // Header['authorization'] is not neccessarily available 
                // when HttpStatusCode.Forbidden happens.
                if (context.HttpResponseStatusCode != HttpStatusCode.Forbidden)
                {
                    CheckAuthorization(context);
                }
            }
            catch(OTSClientException e)
            {
                // re-throw the exception with additonal information
                throw new OTSClientException(
                    String.Format("{0} HTTP Status: {1}.", e.Message, context.HttpResponseStatusCode),
                    context.HttpResponseStatusCode
                );
            }
        }
    }
}
