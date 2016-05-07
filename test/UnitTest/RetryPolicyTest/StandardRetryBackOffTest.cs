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
    class StandardRetryBackOffTest : OTSUnitTestBase
    {
        RetryPolicy retryPolicy = new DefaultRetryPolicy();
        
        void testRetryWithException(OTSServerException[] exceptions)
        {
            foreach (var e in exceptions) {
                OTSClientTestHelper.RetryExceptions.Add(e);
            }
            
            var lastException = exceptions[exceptions.Count() - 1];
            var request = new ListTableRequest();
            
            try {
                OTSClient.ListTable(request);
                Assert.Fail();
            } catch (OTSServerException e) {
                AssertOTSServerException(lastException, e);
            }
            
            Assert.AreEqual(3, OTSClientTestHelper.RetryTimes);
            for (int i = 0; i < OTSClientTestHelper.RetryTimes; i ++) {
                AssertOTSServerException(exceptions[i], OTSClientTestHelper.RetryExceptions[i]);
            }
        }
        
        void testRetry(OTSServerException[] exceptions)
        {
            foreach (var e in exceptions) {
                OTSClientTestHelper.RetryExceptions.Add(e);
            }
            
            var lastException = exceptions[exceptions.Count() - 1];
            var request = new ListTableRequest();
            
            OTSClient.ListTable(request);
            
            Assert.AreEqual(exceptions.Count(), OTSClientTestHelper.RetryTimes);
            for (int i = 0; i < OTSClientTestHelper.RetryTimes; i ++) {
                AssertOTSServerException(exceptions[i], OTSClientTestHelper.RetryExceptions[i]);
            }
        }
        
        void assertRetryDelay(int retryTime, int minDelay, int maxDelay)
        {
            int retryDelay = OTSClientTestHelper.RetryDelays[retryTime];
            Assert.Greater(maxDelay, retryDelay);
            Assert.Greater(retryDelay, minDelay);
        }
        
        // <summary>
        // 测试在出现OTSServerBusy，OTSNotEnoughCapacityUnit 或者  Too frequent table operations错误时的退避符合预期。
        // </summary>
        [Test]
        public void TestRetryBackOffWithServerThrottlingException() 
        {
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            var e1 = new OTSServerException("/ListTable", 
                                            HttpStatusCode.ServiceUnavailable, 
                                            "OTSServerBusy", 
                                            "Server is busy.");
            var e2 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSNotEnoughCapacityUnit", 
                                     "Remaining capacity unit is not enough.");
            var e3 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSQuotaExhausted", 
                                     "Too frequent table operations.");
            
            testRetryWithException(new OTSServerException[]{e1, e1, e1, e1});
            assertRetryDelay(0, 250, 500);
            assertRetryDelay(1, 500, 1000);
            assertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            testRetryWithException(new OTSServerException[]{e2, e2, e2, e2});
            assertRetryDelay(0, 250, 500);
            assertRetryDelay(1, 500, 1000);
            assertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            testRetryWithException(new OTSServerException[]{e3, e3, e3, e3});
            assertRetryDelay(0, 250, 500);
            assertRetryDelay(1, 500, 1000);
            assertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
            
        }

        // <summary>
        // 测试在出现除OTSServerBusy，OTSNotEnoughCapacityUnit 或者  Too frequent table operations以外的错误时的退避符合预期。
        // </summary>
        [Test]
        public void TestRetryBackOffWithOtherExceptions() 
        {
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            var e1 = new OTSServerException("/ListTable", 
                                            HttpStatusCode.ServiceUnavailable, 
                                            "OTSRowOperationConflict", 
                                            "Data is being modified by the other request.");
            var e2 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSTimeout", 
                                     "Operation timeout.");
            
            testRetryWithException(new OTSServerException[]{e1, e1, e1, e1});
            assertRetryDelay(0, 100, 200);
            assertRetryDelay(1, 200, 400);
            assertRetryDelay(2, 400, 800);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            testRetryWithException(new OTSServerException[]{e2, e2, e2, e2});
            assertRetryDelay(0, 100, 200);
            assertRetryDelay(1, 200, 400);
            assertRetryDelay(2, 400, 800);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
        }
        
        [Test]
        public void TestRetryTwice()
        {
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            var e1 = new OTSServerException("/ListTable", 
                                            HttpStatusCode.ServiceUnavailable, 
                                            "OTSRowOperationConflict", 
                                            "Data is being modified by the other request.");
            var e2 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSTimeout", 
                                     "Operation timeout.");
            
            var e3 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSQuotaExhausted", 
                                     "Too frequent table operations.");
            
            testRetry(new OTSServerException[]{e1, e1});
            assertRetryDelay(0, 100, 200);
            assertRetryDelay(1, 200, 400);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            testRetry(new OTSServerException[]{e2, e2});
            assertRetryDelay(0, 100, 200);
            assertRetryDelay(1, 200, 400);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            
            
            testRetry(new OTSServerException[]{e3, e3, e3});
            assertRetryDelay(0, 250, 500);
            assertRetryDelay(1, 500, 1000);
            OTSClientTestHelper.Reset();
            
        }
        
        [Test]
        public void TestRetryWithExceptionChanged()
        {
                        var e1 = new OTSServerException("/ListTable", 
                                            HttpStatusCode.ServiceUnavailable, 
                                            "OTSRowOperationConflict", 
                                            "Data is being modified by the other request.");
            
                        var e3 = new OTSServerException("/ListTable", 
                                     HttpStatusCode.ServiceUnavailable, 
                                     "OTSQuotaExhausted", 
                                     "Too frequent table operations.");
            
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();
            testRetry(new OTSServerException[]{e1, e3, e3});
            assertRetryDelay(0, 100, 200);
            assertRetryDelay(1, 500, 1000);
            assertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
        }

    }
}
