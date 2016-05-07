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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Net;
using System.Net.Http;
using System.IO;

using NUnit.Framework;

using Aliyun.OTS;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;


namespace Aliyun.OTS.UnitTest.ProtocolImplementation
{
    
    [TestFixture]
    class ResponseParseTest : OTSUnitTestBase
    {
            
        // <summary>
        // 在HTTP返回2XX情况下，头部缺少x-ots-contentm5，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestNoContentMD5InHeader() 
        {
            var body = MakeListTableResponseBody();
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body, hasContentMd5:false);
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("x-ots-contentmd5 is missing in response header. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // 在HTTP返回2XX情况下，头部缺少x-ots-requestid，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestNoRequestIDInHeader() 
        {
            var body = MakeListTableResponseBody();
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers.Remove("x-ots-requestid");
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("x-ots-requestid is missing in response header. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // 在HTTP返回2XX情况下，头部缺少x-ots-date，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestNoDateInHeader() 
        {
            var body = MakeListTableResponseBody();
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers.Remove("x-ots-date");
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("x-ots-date is missing in response header. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // 在HTTP返回2XX情况下，头部缺少x-ots-contenttype，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestNoContentTypeInHeader() 
        {
            var body = MakeListTableResponseBody();
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers.Remove("x-ots-contenttype");
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("x-ots-contenttype is missing in response header. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // 在HTTP返回非2XX的情况下，缺少x-ots-contentmd5, x-ots-requestid, x-ots-date, x-ots-contenttype头部，期望正常抛出服务端异常。
        // </summary>
        [Test]
        public void TestHeaderMissingInErrorResponse() 
        {
            var body = MakeErrorPB("Sample Error Code", "Sample Error Message");
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers.Remove("x-ots-contentmd5");
            headers.Remove("x-ots-requestid");
            headers.Remove("x-ots-date");
            headers.Remove("x-ots-contenttype");
            headers["Authorization"] = String.Format("OTS {0}:{1}", 
                                                     TestAccessKeyID,
                                                     MakeSignature("/ListTable", headers, TestAccessKeySecret));
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(
                    new OTSServerException("/ListTable", HttpStatusCode.BadRequest, "Sample Error Code", "Sample Error Message"),
                e);
            }
        }

        // <summary>
        // 在HTTP返回403 (Forbidden) 错误的情况下，包含正常的authorizatiion头部，期望正常抛出服务端异常。
        // </summary>
        [Test]
        public void TestAuthorizationHeaderNormalWhenForbidden() 
        {
            var otsClient = new OTSClient(TestEndPoint, TestAccessKeyID, "abc", TestInstanceName);
            var request = new ListTableRequest();
            
            try {
                otsClient.ListTable(request);
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/ListTable",
                    HttpStatusCode.Forbidden,
                    "OTSAuthFailed",
                    "Signature mismatch."), exception);
            }
        }
        
        // <summary>
        // 在HTTP返回403 (Forbidden) 错误的情况下，缺少authorizatiion头部，期望正常抛出服务端异常。
        // </summary>
        [Test]
        public void TestAuthorizationHeaderMissingWhenForbidden() 
        {            
            var body = MakeErrorPB("Sample Error Code", "Sample Error Message");
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.Forbidden);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers.Remove("x-ots-authorization");
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(
                    new OTSServerException("/ListTable", HttpStatusCode.Forbidden, "Sample Error Code", "Sample Error Message"),
                e);
            }
        }

        // <summary>
        // HTTP返回的x-ots-contentmd5与SDK计算的值不匹配，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestContentMD5MismatchInResponse() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", new byte[20]);
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("MD5 mismatch in response. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // HTTP返回的x-ots-date格式非法，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestInvalidDateInResponse() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers["x-ots-date"] = "Invalid Date String";
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("Invalid date format in response: Invalid Date String HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // HTTP返回的x-ots-date与本地时间间隔超出15分钟，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestDateDifference() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers["x-ots-date"] = (DateTime.UtcNow.AddMinutes(16)).ToString("R");
            headers["Authorization"] = String.Format("OTS {0}:{1}", 
                                                     TestAccessKeyID, 
                                                     MakeSignature("/ListTable", headers, TestAccessKeySecret));
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("The difference between date in response and system time is more than 15 minutes. HTTP Status: OK.", e.Message);
            }
            
            body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            headers = MakeResponseHeaders("/ListTable", body);
            headers["x-ots-date"] = (DateTime.UtcNow.AddMinutes(-16)).ToString("R");
            headers["Authorization"] = String.Format("OTS {0}:{1}", 
                                                     TestAccessKeyID, 
                                                     MakeSignature("/ListTable", headers, TestAccessKeySecret));
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.AreEqual("The difference between date in response and system time is more than 15 minutes. HTTP Status: OK.", e.Message);
            }
        }

        // <summary>
        // HTTP返回的Authorization格式非法，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestInvalidAuthorizationFormat() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers["Authorization"] = String.Format("blahblah");
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.IsTrue(true);
            }
        }

        // <summary>
        // HTTP返回的Authorization头部包含的AccessID与本地不匹配，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestAccessIDInAuthorizationMismatch() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers["Authorization"] = String.Format("OTS {0}:{1}", 
                                                     "AnotherAccessKeyID", 
                                                     MakeSignature("/ListTable", headers, TestAccessKeySecret));
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException) {
                Assert.IsTrue(true);
            }
        }

        // <summary>
        // HTTP返回的Authorization头部包含的签名与本地计算值不匹配，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestSignatureInAuthorizaitionMismatch() 
        {
            var body = MakeListTableResponseBody();
            
            OTSClientTestHelper.SetHTTPResponseBody(body);
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.OK);
            var headers = MakeResponseHeaders("/ListTable", body);
            headers["Authorization"] = String.Format("OTS {0}:{1}", 
                                                    TestAccessKeyID, 
                                                     MakeSignature("/ListTable", headers, "AnotherAccessKeySecret"));
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSClientException e) {
                Assert.IsTrue(true);
            }
        }
    }
}
