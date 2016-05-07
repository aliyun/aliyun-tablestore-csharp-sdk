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

namespace Aliyun.OTS.UnitTest.InterfaceTest
{

    [TestFixture]
    class ClientInitializeTest : OTSUnitTestBase
    {
        // <summary>
        // ClientConfig格式非法，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestInvalidClientConfigFile() { }

        // <summary>
        // ClientConfig缺少必须的字段（AccessKeyID, AccessKeySecret, EndPoint, InstanceName），期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestFieldMissedInClientConfig() { }

        // <summary>
        // 设置MaxConnectionLimit为0或-1，期望抛出客户端异常；设置MaxConnectionLimit为10，校验设置成功。
        // </summary>
        [Test]
        public void TestSetMaxConnectionLimit() { }
        
        // <summary>
        // 禁用debug log
        // </summary>
        [Test]
        public void TestDisableDebugLog()
        {
            var clientConfig = new OTSClientConfig(
                TestEndPoint,
                TestAccessKeyID,
                TestAccessKeySecret,
                TestInstanceName);
            
            clientConfig.OTSDebugLogHandler = null;
            var otsClient = new OTSClient(clientConfig);
            
            var request = new ListTableRequest();
            otsClient.ListTable(request);
        }

        // <summary>
        // 禁用error log
        // </summary>
        [Test]
        public void TestDisableErrorLog()
        {
            var clientConfig = new OTSClientConfig(
                TestEndPoint,
                TestAccessKeyID,
                TestAccessKeySecret,
                TestInstanceName);
            
            clientConfig.OTSErrorLogHandler = null;
            var otsClient = new OTSClient(clientConfig);
            
            var request = new DeleteTableRequest("blahblah");
            try {
                otsClient.DeleteTable(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/DeleteTable",
                    HttpStatusCode.NotFound,
                    "OTSObjectNotExist",
                    "Requested table does not exist."), exception);
            }
        }
    }
}
