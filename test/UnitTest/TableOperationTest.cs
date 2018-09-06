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
using System.Threading;

using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;

namespace Aliyun.OTS.UnitTest
{

    [TestFixture]
    class TableOperationTest : OTSUnitTestBase
    {
        [Test]
        public void CreateTableAndDelete()
        {
            var primaryKeys = new PrimaryKeySchema();
            primaryKeys.Add("PK0", ColumnValueType.String);
            primaryKeys.Add("PK1", ColumnValueType.Integer);
            
            var tableMeta = new TableMeta("SampleTableName", primaryKeys);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request1 = new CreateTableRequest(tableMeta, reservedThroughput);
            var response1 = OTSClient.CreateTable(request1);
            
            var request2 = new ListTableRequest();
            var response2 = OTSClient.ListTable(request2);
            Assert.AreEqual(new List<string>(){"SampleTableName"}, response2.TableNames);
            
            Thread.Sleep(1000);
            var request3 = new DescribeTableRequest("SampleTableName");
            var response3 = OTSClient.DescribeTable(request3);
            Assert.AreEqual("SampleTableName", response3.TableMeta.TableName);
            Assert.AreEqual(primaryKeys, response3.TableMeta.PrimaryKeySchema);
            Assert.AreEqual(reservedThroughput.Read, response3.ReservedThroughputDetails.CapacityUnit.Read);
            Assert.AreEqual(reservedThroughput.Write, response3.ReservedThroughputDetails.CapacityUnit.Write);
            
            
            OTSClient.DeleteTable(new DeleteTableRequest("SampleTableName"));
            
            var request4 = new ListTableRequest();
            var response4 = OTSClient.ListTable(request4);
            Assert.AreEqual(new List<string>(){}, response4.TableNames);
        }
        
        [Test]
        public void UpdateTableAndThenDescribe()
        {
            var primaryKeys = new PrimaryKeySchema();
            primaryKeys.Add("PK0", ColumnValueType.String);
            primaryKeys.Add("PK1", ColumnValueType.Integer);

            var tableMeta = new TableMeta("update_table_and_then_describe", primaryKeys);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request1 = new CreateTableRequest(tableMeta, reservedThroughput);
            var response1 = OTSClient.CreateTable(request1);
            
            //WaitBeforeUpdateTable();
            Thread.Sleep(120*1000);
            var request2 = new UpdateTableRequest(
                "update_table_and_then_describe", 
                new CapacityUnit(1, 1)
            );
            var response2 = OTSClient.UpdateTable(request2);
            
            Assert.AreEqual(0, response2.ReservedThroughputDetails.NumberOfDecreasesToday);
            Assert.AreEqual(1, response2.ReservedThroughputDetails.CapacityUnit.Read);
            Assert.AreEqual(1, response2.ReservedThroughputDetails.CapacityUnit.Write);
            
        }
    }
}
