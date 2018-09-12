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
using Aliyun.OTS.Retry;
using Aliyun.OTS.Handler;

namespace Aliyun.OTS.UnitTest.RetryPolicyTest
{

    [TestFixture]
    class StandardRetryConditionTest : OTSUnitTestBase
    {
        RetryPolicy retryPolicy = new DefaultRetryPolicy();
        private string[] apiNames = new [] {
            "CreateTable",   // 0
            "ListTable",     // 1
            "UpdateTable",   // 2
            "DescribeTable", // 3
            "DeleteTable",   // 4
            "GetRow",        // 5
            "PutRow",        // 6
            "UpdateRow",     // 7
            "DeleteRow",     // 8
            "BatchGetRow",   // 9
            "BatchWriteRow", // 10
            "GetRange",      // 11
        };
        private string[][] errors = {
            new []{ "OTSAuthFailed", null },                                            // 0
            new []{ "OTSRequestBodyTooLarge", null },                                   // 1
            new []{ "OTSRequestTimeout", null },                                        // 2
            new []{ "OTSMethodNotAllowed", null },                                      // 3
            new []{ "OTSParameterInvalid", null },                                      // 4
            new []{ "OTSInternalServerError", null },                                   // 5
            new []{ "OTSQuotaExhausted", "Too frequent table operations." },            // 6
            new []{"OTSQuotaExhausted", "Number of tables exceeded the quota."},        // 7
            new []{ "OTSServerBusy", null },                                            // 8
            new []{ "OTSPartitionUnavailable", null },                                  // 9
            new []{ "OTSTimeout", null },                                               // 10
            new []{ "OTSServerUnavailable", null },                                     // 11
            new []{ "OTSRowOperationConflict", null },                                  // 12
            new []{ "OTSObjectAlreadyExist", null },                                    // 13
            new []{ "OTSObjectNotExist", null },                                        // 14
            new []{ "OTSTableNotReady", null },                                         // 15
            new []{ "OTSTooFrequentReservedThroughputAdjustment", null },               // 16
            new []{ "OTSNotEnoughCapacityUnit", null },                                 // 17
            new []{ "OTSConditionCheckFail", null },                                    // 18
            new []{ "OTSOutOfRowSizeLimit", null },                                     // 19
            new []{ "OTSOutOfColumnCountLimit", null },                                 // 20
            new []{ "OTSInvalidPK", null },                                             // 21
        };
            
        int[][] expectRetryTable = {
            //      0  1  2  3  4  5  6  7  8  9  10 11
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 0
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 1
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 2
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 3
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 4
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 5
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 6
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 7
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 8
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 9
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 10
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 11
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 12
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 13
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 14
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 15
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 16
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 17
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 18
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 19
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 20
            new []{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },    // 21
        };
        
        int[][] expectRetryTableWith5XX = {
            //      0  1  2  3  4  5  6  7  8  9  10 11
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 0
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 1
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 2
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 3
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 4
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 5
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 6
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 7
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 8
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 9
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 10
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 11
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 12
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 13
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 14
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 15
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 16
            new []{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },    // 17
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 18
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 19
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 20
            new []{ 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1 },    // 21
        };
        
        /// <summary>
        /// 测试标准重试策略在所有API在所有出错情况下是否重试。
        /// </summary>
        [Test]
        public void TestStandardRetryCondition()
        {
            TestRetryCondition(expectRetryTable, HttpStatusCode.BadRequest);
            TestRetryCondition(expectRetryTable, HttpStatusCode.Forbidden);
            TestRetryCondition(expectRetryTableWith5XX, HttpStatusCode.InternalServerError);
            TestRetryCondition(expectRetryTableWith5XX, HttpStatusCode.ServiceUnavailable);
            TestRetryCondition(expectRetryTableWith5XX, HttpStatusCode.BadGateway);
        }
        
        void TestRetryCondition(int[][] retryConditionTable, HttpStatusCode httpCode)
        {
            for (int i = 0; i < apiNames.Count(); i ++) {
                for (int j = 0; j < errors.Count(); j ++) {
                    var apiName = "/" + apiNames[i];
                    var errorCode = errors[j][0];
                    var errorMessage = errors[j][1];
                    var expect = retryConditionTable[j][i] == 1;
                    
                    var exception = new OTSServerException(apiName, httpCode, errorCode, errorMessage);
                    var context = new Context
                    {
                        APIName = apiName
                    };

                    Assert.AreEqual(expect, retryPolicy.CanRetry(context, exception), 
                                    "Retry Condition Not Match: API Name {0}, Error Code: {1}",
                                    apiName, errorCode);
                }
            }
        }
    }
}
