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

using NUnit.Framework;
using Aliyun.OTS.Request;


namespace Aliyun.OTS.UnitTest.ProtocolImplementation
{

    [TestFixture]
    class ErrorHandlingTest : OTSUnitTestBase 
    {

        // <summary>
        // 服务器返回的 message Error 不合法，期望抛出服务端异常，并校验 HTTP 返回码。
        // </summary>
        [Test]
        public void TestInvalidPBInError()
        {
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            OTSClientTestHelper.SetHTTPResponseBody(new byte[] { });

            var request = new ListTableRequest();

            try
            {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            }
            catch (OTSServerException e)
            {
                AssertOTSServerException(new OTSServerException("/ListTable", HttpStatusCode.BadRequest), e);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        // <summary>
        // 服务器返回的 message Error 不合法，期望抛出服务端异常，并校验 HTTP 返回码。
        // </summary>
        [Test]
        public void TestInvalidPBInErrorWithCorrectBody() 
            {
            
            var body = new byte[20];
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            OTSClientTestHelper.SetHTTPResponseBody(body);
            var headers = MakeResponseHeaders("/ListTable", body);
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(new OTSServerException("/ListTable", HttpStatusCode.BadRequest), e);
            }
        }

        // <summary>
        // 服务器返回的 ErrorCode 包含Unicode字符，期望正常抛出。
        // </summary>
        [Test]
        public void TestUnicodeInErrorCode() 
        {
            var body = MakeErrorPB("中文错误码", "Sample Error Message");
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            OTSClientTestHelper.SetHTTPResponseBody(body);
            var headers = MakeResponseHeaders("/ListTable", body);
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(
                    new OTSServerException("/ListTable", HttpStatusCode.BadRequest, "中文错误码", "Sample Error Message"), 
                e);
                Assert.AreEqual("fake-request-id-for-test", e.RequestID);
            }
        }

        // <summary>
        // 服务器返回的 ErrorMessage 包含Unicode字符，期望正常抛出。
        // </summary>
        [Test]
        public void TestUnicodeInErrorMessage() 
        {
            var body = MakeErrorPB("Sample Error Code", "中文错误信息");
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            OTSClientTestHelper.SetHTTPResponseBody(body);
            var headers = MakeResponseHeaders("/ListTable", body);
            OTSClientTestHelper.SetHttpRequestHeaders(headers);
            
            var request = new ListTableRequest();
            
            try {
                var response = OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(
                    new OTSServerException("/ListTable", HttpStatusCode.BadRequest, "Sample Error Code", "中文错误信息"),
                e);
                Assert.AreEqual("fake-request-id-for-test", e.RequestID);
            }
        }

        // <summary>
        // 在返回为非2XX的情况下，headers没有包含x-ots-requestid，期望正常抛出。
        // </summary>
        [Test]
        public void TestNoRequestIDInErrorResponse() 
        {
            var body = MakeErrorPB("Sample Error Code", "Sample Error Message");
            OTSClientTestHelper.SetHttpStatusCode(HttpStatusCode.BadRequest);
            OTSClientTestHelper.SetHTTPResponseBody(body);
            var headers = MakeResponseHeaders("/ListTable", body, hasRequestID:false);
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

    }
}
