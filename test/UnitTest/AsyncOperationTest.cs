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

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class AsyncOperationTest : OTSUnitTestBase
    {
        // <summary>
        // 先启动1000个PutRow请求并等待结束，再启动1000个GetRow请求并等待结束。
        // </summary>
        [Test]
        public void TestAsyncOperations()
        {
            int requestCount = 100;
            var clientConfig = new OTSClientConfig(TestEndPoint, TestAccessKeyID, TestAccessKeySecret, TestInstanceName)
            {
                OTSDebugLogHandler = null,
                OTSErrorLogHandler = null
            };

            OTSClient = new OTSClient(clientConfig);
            OTSClientTestHelper.Reset();

            CreateTestTableWith4PK(new CapacityUnit(0, 0));

            var putRowTaskList = new List<Task<PutRowResponse>>();
            for (int i = 0; i < requestCount; i++)
            {
                var request = new PutRowRequest(TestTableName, new Condition(RowExistenceExpectation.IGNORE));
                request.RowPutChange.PrimaryKey = GetPredefinedPrimaryKeyWith4PK(i);
                request.RowPutChange.AddColumns(GetPredefinedAttributeWith5PK(i));
                putRowTaskList.Add(OTSClient.PutRowAsync(request));
            }

            foreach (var task in putRowTaskList)
            {
                task.Wait();
                AssertCapacityUnit(new CapacityUnit(0, 1), task.Result.ConsumedCapacityUnit);
            }

            var getRowTaskList = new List<Task<GetRowResponse>>();
            for (int i = 0; i < requestCount; i++)
            {
                var request = new GetRowRequest(TestTableName,
                                                GetPredefinedPrimaryKeyWith4PK(i));
                getRowTaskList.Add(OTSClient.GetRowAsync(request));
            }

            for (int i = 0; i < requestCount; i++)
            {
                var task = getRowTaskList[i];
                task.Wait();
                var response = task.Result;
                AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
                AssertPrimaryKey(GetPredefinedPrimaryKeyWith4PK(i), response.PrimaryKey);
                AssertAttribute(GetPredefinedAttributeWith5PK(i), response.Attribute);
            }
        }
    }
}
