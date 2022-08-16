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

using NUnit.Framework;
using Aliyun.OTS.Request;
using System;

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
            } catch (Exception)
            {
                
            }
            
            try{
                var otsClient = new OTSClient("http://10.10.10.10", "abc", "def", "ghi");
                var request = new ListTableRequest();
                var response = otsClient.ListTable(request);
                Assert.Fail();
            } catch (Exception){
                
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
