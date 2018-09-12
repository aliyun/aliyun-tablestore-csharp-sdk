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

using System.Linq;
using System.Net;

using NUnit.Framework;
using Aliyun.OTS.Request;
using Aliyun.OTS.Retry;

namespace Aliyun.OTS.UnitTest.RetryPolicyTest
{
    [TestFixture]
    class StandardRetryBackOffTest : OTSUnitTestBase
    {
        readonly RetryPolicy retryPolicy = new DefaultRetryPolicy();

        private void TestRetryWithException(OTSServerException[] exceptions)
        {
            foreach (var e in exceptions)
            {
                OTSClientTestHelper.RetryExceptions.Add(e);
            }

            var lastException = exceptions[exceptions.Count() - 1];
            var request = new ListTableRequest();

            try
            {
                OTSClient.ListTable(request);
                Assert.Fail();
            }
            catch (OTSServerException e)
            {
                AssertOTSServerException(lastException, e);
            }

            Assert.AreEqual(3, OTSClientTestHelper.RetryTimes);
            for (int i = 0; i < OTSClientTestHelper.RetryTimes; i++)
            {
                AssertOTSServerException(exceptions[i], OTSClientTestHelper.RetryExceptions[i]);
            }
        }

        private void TestRetry(OTSServerException[] exceptions)
        {
            foreach (var e in exceptions)
            {
                OTSClientTestHelper.RetryExceptions.Add(e);
            }

            var lastException = exceptions[exceptions.Count() - 1];
            var request = new ListTableRequest();

            OTSClient.ListTable(request);

            Assert.AreEqual(exceptions.Count(), OTSClientTestHelper.RetryTimes);
            for (int i = 0; i < OTSClientTestHelper.RetryTimes; i++)
            {
                AssertOTSServerException(exceptions[i], OTSClientTestHelper.RetryExceptions[i]);
            }
        }

        private void AssertRetryDelay(int retryTime, int minDelay, int maxDelay)
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

            TestRetryWithException(new OTSServerException[] { e1, e1, e1, e1 });
            AssertRetryDelay(0, 250, 500);
            AssertRetryDelay(1, 500, 1000);
            AssertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();

            TestRetryWithException(new OTSServerException[] { e2, e2, e2, e2 });
            AssertRetryDelay(0, 250, 500);
            AssertRetryDelay(1, 500, 1000);
            AssertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();

            TestRetryWithException(new OTSServerException[] { e3, e3, e3, e3 });
            AssertRetryDelay(0, 250, 500);
            AssertRetryDelay(1, 500, 1000);
            AssertRetryDelay(2, 1000, 2000);
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

            TestRetryWithException(new OTSServerException[] { e1, e1, e1, e1 });
            AssertRetryDelay(0, 100, 200);
            AssertRetryDelay(1, 200, 400);
            AssertRetryDelay(2, 400, 800);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();

            TestRetryWithException(new OTSServerException[] { e2, e2, e2, e2 });
            AssertRetryDelay(0, 100, 200);
            AssertRetryDelay(1, 200, 400);
            AssertRetryDelay(2, 400, 800);
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

            TestRetry(new OTSServerException[] { e1, e1 });
            AssertRetryDelay(0, 100, 200);
            AssertRetryDelay(1, 200, 400);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();

            TestRetry(new OTSServerException[] { e2, e2 });
            AssertRetryDelay(0, 100, 200);
            AssertRetryDelay(1, 200, 400);
            OTSClientTestHelper.Reset();
            OTSClientTestHelper.TurnOnRetryTimesAndBackOffRecording();


            TestRetry(new OTSServerException[] { e3, e3, e3 });
            AssertRetryDelay(0, 250, 500);
            AssertRetryDelay(1, 500, 1000);
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
            TestRetry(new OTSServerException[] { e1, e3, e3 });
            AssertRetryDelay(0, 100, 200);
            AssertRetryDelay(1, 500, 1000);
            AssertRetryDelay(2, 1000, 2000);
            OTSClientTestHelper.Reset();
        }

    }
}
