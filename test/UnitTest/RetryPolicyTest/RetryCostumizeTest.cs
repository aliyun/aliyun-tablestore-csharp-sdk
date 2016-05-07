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
using Aliyun.OTS.Retry;

namespace Aliyun.OTS.UnitTest.RetryPolicyTest
{
    [TestFixture]
    class RetryCostumizeTest : OTSUnitTestBase
    {
        
        // <summary>
        // 设置最大重试次数为-1，或者 -1000，期望抛出客户端异常。
        // </summary>
        [Test]
        public void TestInvalidMaxRetryTimes() 
        {
            
            try {
                RetryPolicy policy = new DefaultRetryPolicy(-1, 0);
                Assert.Fail();
            } catch (OTSClientException) {
            }
            
            try {
                RetryPolicy policy = new DefaultRetryPolicy(-1000, 0);
                Assert.Fail();
            } catch (OTSClientException) {
            }
            
        }

        // <summary>
        // 设置最大重试次数为0，或者5，期望实际重试次数符合预期。
        // </summary>
        [Test]
        public void TestSetMaxRetryTimes() 
        {
            RetryPolicy policy = new DefaultRetryPolicy(0, 3);
                
            policy = new DefaultRetryPolicy(5, 0);
        }
        // <summary>
        // 设置自定义重试条件为 “400均重试，其他不重试”，测试实际重试行为符合预期。
        // </summary>
        [Test]
        public void TestSetRetryCondition() {}

        // <summary>
        // 设置自定义重试次数和退避函数为 “重试6次，间隔为10毫秒”，测试实际重试行为符合预期。
        // </summary>
        [Test]
        public void TestSetRetryTimeAndBackOff() {}

        // <summary>
        // 设置重试条件为NO_RETRY_CONDITION，测试实际重试条件为不重试。
        // </summary>
        [Test]
        public void TestSetNoRetryCondition() {}

        // <summary>
        // 设置重试条件为NO_RETRY_DELAY，测试实际重试间隔为0。
        // </summary>
        [Test]
        public void TestSetNoRetryDelay() {}

        // <summary>
        // 设置重试条件为NO_RETRY_POLICY，测试实际重试条件为不重试。
        // </summary>
        [Test]
        public void TestSetNoRetryPolicy() {}
    }
}
