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

using NUnit.Framework;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel;
using System.Threading;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class SampleCodeTest : OTSUnitTestBase
    {
        [Test]
        public void ListTableTest()
        {
            var otsClient = OTSClient;
            
            var request = new ListTableRequest();
            var response = otsClient.ListTable(request);
            
            foreach (var tableName in response.TableNames) {
                // Do something
            }
        }
        
        private void CreateTable()
        {
            var otsClient = OTSClient;
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.String);
            primaryKeySchema.Add("PK1", ColumnValueType.Integer);

            var tableMeta = new TableMeta("SampleTable", primaryKeySchema);

            var reservedThroughput = new CapacityUnit(0, 0);
            var request = new CreateTableRequest(tableMeta, reservedThroughput);
            var response = otsClient.CreateTable(request);
            WaitForTableReady();
        }
        
        [Test]
        public void UpdateTableTest()
        {
            CreateTable();
            var otsClient = OTSClient;

            Thread.Sleep(120 * 1000);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request = new UpdateTableRequest("SampleTable", reservedThroughput);
            var response = otsClient.UpdateTable(request);
            DeleteTable();
        }
        
        [Test]
        public void GetRowTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));

            var attribute = new AttributeColumns();
            attribute.Add("IntAttr0", new ColumnValue(12345));
            attribute.Add("StringAttr1", new ColumnValue("ABC"));
            attribute.Add("DoubleAttr2", new ColumnValue(3.14));
            attribute.Add("BooleanAttr3", new ColumnValue(true));
            var putRowRequest = new PutRowRequest(
                           "SampleTable",
                           new Condition(RowExistenceExpectation.IGNORE),
                           primaryKey,
                           attribute
                       );
            var putRowResponse = otsClient.PutRow(putRowRequest);

            var getRowRequest = new GetRowRequest(
                                    "SampleTable",
                                    primaryKey
                                );
            var getRowResponse = otsClient.GetRow(getRowRequest);
          
            System.Console.WriteLine("GetRow CU Consumed: Read {0} Write {0}",
                getRowResponse.ConsumedCapacityUnit.Read,
                getRowResponse.ConsumedCapacityUnit.Write);
         
            var pk0 = getRowResponse.PrimaryKey["PK0"];
            System.Console.WriteLine("PrimaryKey PK0 Value {0}", pk0.StringValue);
            var pk1 = getRowResponse.PrimaryKey["PK1"];
            System.Console.WriteLine("PrimaryKey PK1 Value {0}", pk1.IntegerValue);
            var attr0 = getRowResponse.Attribute["IntAttr0"];
            System.Console.WriteLine("Attribute IntAttr0 Value {0}", attr0.IntegerValue);
            var attr1 = getRowResponse.Attribute["StringAttr1"];
            System.Console.WriteLine("Attribute StringAttr1 Value {0}", attr1.StringValue);
            var attr2 = getRowResponse.Attribute["DoubleAttr2"];
            System.Console.WriteLine("Attribute DoubleAttr2 Value {0}", attr2.DoubleValue);
            var attr3 = getRowResponse.Attribute["BooleanAttr3"];
            System.Console.WriteLine("Attribute BooleanAttr3 Value {0}", attr2.BooleanValue);
            DeleteTable();
        }
        
        [Test]
        public void PutRowTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
			
            var attribute = new AttributeColumns();
            attribute.Add("IntAttr0", new ColumnValue(12345));
            attribute.Add("StringAttr1", new ColumnValue("ABC"));
            attribute.Add("DoubleAttr2", new ColumnValue(3.14));
            attribute.Add("BooleanAttr3", new ColumnValue(true));
			
            var putRowRequest = new PutRowRequest(
                           "SampleTable",
                           new Condition(RowExistenceExpectation.IGNORE),
                           primaryKey,
                           attribute
                       );
			
            var putRowResponse = otsClient.PutRow(putRowRequest);
            System.Console.WriteLine("PutRow CU Consumed: Read {0} Write {0}",
                putRowResponse.ConsumedCapacityUnit.Read,
                putRowResponse.ConsumedCapacityUnit.Write);
            DeleteTable();
        }
        
        [Test]
        public void UpdateRowTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            var updateOfAttribute = new UpdateOfAttribute();
            updateOfAttribute.AddAttributeColumnToPut("NewColumn", new ColumnValue(123));
            updateOfAttribute.AddAttributeColumnToDelete("IntAttr0");
            
            var updateRowRequest = new UpdateRowRequest(
                                       "SampleTable",
                                       new Condition(RowExistenceExpectation.IGNORE),
                                       primaryKey,
                                       updateOfAttribute);
            var updateRowResponse = otsClient.UpdateRow(updateRowRequest);
            
            System.Console.WriteLine("UpdateRow CU Consumed: Read {0} Write {0}",
                updateRowResponse.ConsumedCapacityUnit.Read,
                updateRowResponse.ConsumedCapacityUnit.Write);
            DeleteTable();
        }
        
        [Test]
        public void DeleteRowTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var deleteRowRequest = new DeleteRowRequest(
                                                   "SampleTable",
                                                   new Condition(RowExistenceExpectation.IGNORE),
                                                   primaryKey);
            var deleteRowResponse = otsClient.DeleteRow(deleteRowRequest);            
            System.Console.WriteLine("DeleteRow CU Consumed: Read {0} Write {0}",
                deleteRowResponse.ConsumedCapacityUnit.Read,
                deleteRowResponse.ConsumedCapacityUnit.Write);
            DeleteTable();
        }
        
        [Test]
        public void BatchWriteRowTest()
        {
            var otsClient = OTSClient;
            
            var primaryKey1 = new PrimaryKey();
            primaryKey1.Add("PK0", new ColumnValue("TestData"));
            primaryKey1.Add("PK1", new ColumnValue(0));
            
            var attribute1 = new AttributeColumns();
            attribute1.Add("Col0", new ColumnValue("Hangzhou"));
            
            var primaryKey2 = new PrimaryKey();
            primaryKey2.Add("PK0", new ColumnValue("TestData"));
            primaryKey2.Add("PK1", new ColumnValue(1));
            
            var attribute2 = new AttributeColumns();
            attribute2.Add("Col0", new ColumnValue("Shanghai"));
            
            var rowChanges = new RowChanges();
            rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey1, attribute1);
            rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey2, attribute2);
            var batchWriteRowRequest = new BatchWriteRowRequest();
            batchWriteRowRequest.Add("SampleTableName", rowChanges);
            
            var batchWriteRowResponse = otsClient.BatchWriteRow(batchWriteRowRequest);
            
            foreach (var responseForOneTable in batchWriteRowResponse.TableRespones) {
                foreach (var row in responseForOneTable.Value.PutResponses) {
                    // 处理每一行的返回
                }
            }
        }
        
        private void DeleteTable()
        {
            var otsClient = OTSClient;

            var deleteTableRequest = new DeleteTableRequest("SampleTable");
            otsClient.DeleteTable(deleteTableRequest);
        }
        
        [Test]
        public void BatchGetRowTest()
        {
            var otsClient = OTSClient;
            
            var primaryKey1 = new PrimaryKey();
            primaryKey1.Add("PK0", new ColumnValue("TestData"));
            primaryKey1.Add("PK1", new ColumnValue(0));
            
            var primaryKey2 = new PrimaryKey();
            primaryKey2.Add("PK0", new ColumnValue("TestData"));
            primaryKey2.Add("PK1", new ColumnValue(1));
            
            var batchGetRowRequest = new BatchGetRowRequest();
            var primaryKeys = new List<PrimaryKey>() {
                primaryKey1,
                primaryKey2
            };
            batchGetRowRequest.Add("SampleTableName", primaryKeys);
            var batchGetRowRespnse = otsClient.BatchGetRow(batchGetRowRequest);
            
            foreach (var responseForOneTable in batchGetRowRespnse.RowDataGroupByTable) {
                foreach (var row in responseForOneTable.Value) {
                    // 处理每一行的返回
                }
            }
        }
        
        [Test]
        public void GetRangeTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            // 指定范围读取数据
            var startPrimaryKey = new PrimaryKey();
            startPrimaryKey.Add("PK0", new ColumnValue("TestData"));
            startPrimaryKey.Add("PK1", ColumnValue.INF_MIN);
            
            var endPrimaryKey = new PrimaryKey();
            endPrimaryKey.Add("PK0", new ColumnValue("TestData"));
            endPrimaryKey.Add("PK1", ColumnValue.INF_MAX);
            
            var consumed = new CapacityUnit(0, 0);

            var request = new GetIteratorRequest("SampleTable", GetRangeDirection.Forward,
                                    startPrimaryKey, endPrimaryKey,
                                    consumed);
            var iterator = OTSClient.GetRangeIterator(request);
                
            foreach (var rowData in iterator) {
                // 处理每一行数据
            }
            DeleteTable();
        }
    }
}
