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
            string tableName = "SampleTableName";

            var primaryKeys = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.Integer }
            };

            var tableOption = new TableOptions
            {
                MaxVersions =  1,
                TimeToLive = -1
            };

            var tableMeta = new TableMeta(tableName, primaryKeys);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request1 = new CreateTableRequest(tableMeta, reservedThroughput)
            {
                TableOptions = tableOption
            };

            var response1 = OTSClient.CreateTable(request1);
            
            var request2 = new ListTableRequest();
            var response2 = OTSClient.ListTable(request2);
            Assert.IsTrue(response2.TableNames.Contains(tableName));
            
            Thread.Sleep(1000);
            var request3 = new DescribeTableRequest(tableName);
            var response3 = OTSClient.DescribeTable(request3);
            Assert.AreEqual(tableName, response3.TableMeta.TableName);
            Assert.AreEqual(primaryKeys, response3.TableMeta.PrimaryKeySchema);
            Assert.AreEqual(reservedThroughput.Read, response3.ReservedThroughputDetails.CapacityUnit.Read);
            Assert.AreEqual(reservedThroughput.Write, response3.ReservedThroughputDetails.CapacityUnit.Write);
            
            
            OTSClient.DeleteTable(new DeleteTableRequest(tableName));
            
            var request4 = new ListTableRequest();
            var response4 = OTSClient.ListTable(request4);
            Assert.IsFalse(response4.TableNames.Contains(tableName));
        }
        
        [Test]
        public void UpdateTableAndThenDescribe()
        {
            string tableName = "update_table_and_then_describe";
            var primaryKeys = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.Integer }
            };

            var tableMeta = new TableMeta(tableName, primaryKeys);
            var reservedThroughput = new CapacityUnit(0, 0);

            var tableOption = new TableOptions
            {
                MaxVersions = 1,
                TimeToLive = -1
            };


            var request1 = new CreateTableRequest(tableMeta, reservedThroughput)
            {
                TableOptions = tableOption
            };

            var response1 = OTSClient.CreateTable(request1);
            
            WaitBeforeUpdateTable();

            var request2 = new UpdateTableRequest(tableName)
            {
                TableOptions = tableOption
            };

            var response2 = OTSClient.UpdateTable(request2);

             OTSClient.DeleteTable(new DeleteTableRequest(tableName));
            
            Assert.AreEqual(0, response2.ReservedThroughputDetails.NumberOfDecreasesToday);
            Assert.AreEqual(0, response2.ReservedThroughputDetails.CapacityUnit.Read);
            Assert.AreEqual(0, response2.ReservedThroughputDetails.CapacityUnit.Write);
        }
    }
}
