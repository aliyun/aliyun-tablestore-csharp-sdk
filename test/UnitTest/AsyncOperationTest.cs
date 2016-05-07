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
            var clientConfig = new OTSClientConfig(
                                               TestEndPoint,
                                               TestAccessKeyID,
                                               TestAccessKeySecret,
                                               TestInstanceName
                                           );
            clientConfig.OTSDebugLogHandler = null;
            clientConfig.OTSErrorLogHandler = null;
            OTSClient = new OTSClient(clientConfig);
            OTSClientTestHelper.Reset();
            
            CreateTestTableWith4PK(new CapacityUnit(0, 0));
            
            var putRowTaskList = new List<Task<PutRowResponse>>();
            for (int i = 0; i < 1000; i ++)
            {
                var request = new PutRowRequest(TestTableName, new Condition(RowExistenceExpectation.IGNORE), 
                                                GetPredefinedPrimaryKeyWith4PK(i),
                                                GetPredefinedAttributeWith5PK(i));
                putRowTaskList.Add(OTSClient.PutRowAsync(request));
            }
            
            foreach (var task in putRowTaskList)
            {
                task.Wait();
                AssertCapacityUnit(new CapacityUnit(0, 1), task.Result.ConsumedCapacityUnit);
            }
            
            var getRowTaskList = new List<Task<GetRowResponse>>();
            for (int i = 0; i < 1000; i++) {
                var request = new GetRowRequest(TestTableName, 
                                                GetPredefinedPrimaryKeyWith4PK(i));
                getRowTaskList.Add(OTSClient.GetRowAsync(request));
            }
            
            for (int i = 0; i < 1000; i++) {
                var task = getRowTaskList[i];
                task.Wait();
                var response = task.Result;
                AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
                AssertPrimaryKey(GetPredefinedPrimaryKeyWith4PK(i), response.PrimaryKey);
                AssertAttribute(GetPredefinedAttributeWith5PK(i), response.Attribute);
            }
        }

        // <summary>
        // 先启动1000个PutRow请求，并且都包含CallBack函数，并等待结束。校验CallBack函数执行成功。
        // </summary>
        [Test]
        public void TestAsyncOperationsWithCallBacks() { }

    }
}
