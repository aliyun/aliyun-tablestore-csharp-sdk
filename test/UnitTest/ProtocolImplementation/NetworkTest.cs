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
    public class NetworkTest
    {

        // <summary>
        // OTSClient的EndPoint配置了一个不存在的HTTP地址，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestServerNotAvaliable() 
        {
            try{
                var otsClient = new OTSClient("http://blahblah", "abc", "def", "ghi");
                var request = new ListTableRequest();
                var response = otsClient.ListTable(request);
                Assert.Fail();
            } catch (HttpRequestException) {
                
            }
            
            try{
                var otsClient = new OTSClient("http://10.10.10.10", "abc", "def", "ghi");
                var request = new ListTableRequest();
                var response = otsClient.ListTable(request);
                Assert.Fail();
            } catch (HttpRequestException) {
                
            }
        }

        // <summary>
        // SDK请求发送后服务端断开连接，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestServerDisconnected() {}

        // <summary>
        // SDK请求发送后在timeout时间内未返回，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestServerNoResponse() {}

    }
}
