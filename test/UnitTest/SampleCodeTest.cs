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
using System;

namespace Aliyun.OTS.UnitTest
{
    /// <summary>
    /// Sample code test. This is Not unit Test
    /// </summary>
    [TestFixture]
    class SampleCodeTest : OTSUnitTestBase
    {
        [Test]
        public void CreateAutoIncrementColumnTableTest()
        {
            var schema = new PrimaryKeySchema
                {
                { "PK0", ColumnValueType.String },
                    { "PK1", ColumnValueType.Integer, PrimaryKeyOption.AUTO_INCREMENT }
            };
            CreateAutoIncrementColumnTable(schema);
            DeleteTable();
        }

        [Test]
        public void CreateMultiAutoIncrementColumnTableTest_ShouldFailed()
        {
            var schema = new PrimaryKeySchema
                {
                    { "PK0", ColumnValueType.String },
                    { "PK1", ColumnValueType.Integer, PrimaryKeyOption.AUTO_INCREMENT },
                    { "PK2", ColumnValueType.Integer, PrimaryKeyOption.AUTO_INCREMENT }
            };
            var tableMeta = new TableMeta(TestTableName, schema);

            var tableOptions = new TableOptions
            {
                MaxVersions = 10,
                TimeToLive = -1
            };
            var reservedThroughput = new CapacityUnit(0, 0);

            var request = new CreateTableRequest(tableMeta, reservedThroughput)
            {
                TableOptions = tableOptions
            };

            try{
                OTSClient.CreateTable(request);
                WaitForTableReady();
            }catch(Exception e){
                Assert.IsTrue(e.Message.Contains("AUTO_INCREMENT primary key count must <= 1"));
            }
        }

        [Test]
        public void PutRowWithAutoIncrementColumnTableTest()
        {
            var schema = new PrimaryKeySchema
                {
                { "PK0", ColumnValueType.String },
                    { "PK1", ColumnValueType.Integer, PrimaryKeyOption.AUTO_INCREMENT }
            };

            CreateAutoIncrementColumnTable(schema);

            var otsClient = OTSClient;
            var putRowRequest = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE)
            );

            // put first row
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", ColumnValue.AUTO_INCREMENT }
            };

            var attribute1 = new Column("Col0", new ColumnValue("Hangzhou"));

            putRowRequest.RowPutChange = new RowPutChange(TestTableName, primaryKey);
            putRowRequest.RowPutChange.AddColumn(attribute1);

            var putRowResponse = otsClient.PutRow(putRowRequest);
            Console.WriteLine("PutRow CU Consumed: Read {0} Write {1}",
                putRowResponse.ConsumedCapacityUnit.Read,
                putRowResponse.ConsumedCapacityUnit.Write);

            // put second row
            primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("CDE") },
                { "PK1", ColumnValue.AUTO_INCREMENT }
            };

            attribute1 = new Column("Col0", new ColumnValue("Beijing"));

            putRowRequest.RowPutChange = new RowPutChange(TestTableName, primaryKey);
            putRowRequest.RowPutChange.AddColumn(attribute1);

            putRowResponse = otsClient.PutRow(putRowRequest);
            Console.WriteLine("PutRow CU Consumed: Read {0} Write {1}",
                putRowResponse.ConsumedCapacityUnit.Read,
                putRowResponse.ConsumedCapacityUnit.Write);

        }


        [Test]
        public void ListTableTest()
        {
            var otsClient = OTSClient;
            var request = new ListTableRequest();
            var response = otsClient.ListTable(request);
            Assert.GreaterOrEqual(response.TableNames.Count, 0);
        }

        [Test]
        public void DescribeTableTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            var request = new DescribeTableRequest(TestTableName);
            var response = otsClient.DescribeTable(request);
            Assert.AreEqual(response.TableMeta.TableName, TestTableName);
            DeleteTable();
        }

        [Test]
        public void UpdateTableTest()
        {
            CreateTable();
            var otsClient = OTSClient;
            var request = new UpdateTableRequest(TestTableName)
            {
                TableOptions = new TableOptions()
            };

            request.TableOptions.MaxVersions = 2;
            request.TableOptions.TimeToLive = -1;

            var response = otsClient.UpdateTable(request);
            Assert.Pass();
            DeleteTable();
        }

        [Test]
        public void PutRowTest()
        {
            CreateTable();
            PutRow();
            DeleteTable();
        }

        [Test]
        public void GetRowTest()
        {
            CreateTable();
            PutRow();
            var otsClient = OTSClient;
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var getRowRequest = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            getRowRequest.QueryCriteria.MaxVersions = 1;

            var getRowResponse = otsClient.GetRow(getRowRequest);

            Console.WriteLine("GetRow CU Consumed: Read {0} Write {1}",
                getRowResponse.ConsumedCapacityUnit.Read,
                getRowResponse.ConsumedCapacityUnit.Write);

            var pk0 = getRowResponse.PrimaryKey["PK0"];
            Console.WriteLine("PrimaryKey PK0 Value {0}", pk0.StringValue);
            var pk1 = getRowResponse.PrimaryKey["PK1"];
            Console.WriteLine("PrimaryKey PK1 Value {0}", pk1.IntegerValue);

            var attr0 = getRowResponse.Attribute["Col0"];
            Console.WriteLine("Attribute Col0 Value {0}", attr0.StringValue);
            var attr1 = getRowResponse.Attribute["Col1"];
            Console.WriteLine("Attribute Col1 Value {0}", attr1.BooleanValue);
            var attr2 = getRowResponse.Attribute["Col2"];
            Console.WriteLine("Attribute Col2 Value {0}", attr2.DoubleValue);

            DeleteTable();
        }

        [Test]
        public void GetTimeRangeRowTest()
        {
            CreateTable();
            PutRow();
            var otsClient = OTSClient;
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            // update row
            var updateOfAttribute = new UpdateOfAttribute();
            updateOfAttribute.AddAttributeColumnToPut("Col0", new ColumnValue("Beijing"));

            var updateRowRequest = new UpdateRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                updateOfAttribute);
            var updateRowResponse = otsClient.UpdateRow(updateRowRequest);

            var getRowRequest = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            getRowRequest.QueryCriteria.TimeRange = new TimeRange
            {
                StartTime = 0,
                EndTime = Int64.MaxValue
            };

            var getRowResponse = otsClient.GetRow(getRowRequest);

            Console.WriteLine("GetRow CU Consumed: Read {0} Write {1}",
                getRowResponse.ConsumedCapacityUnit.Read,
                getRowResponse.ConsumedCapacityUnit.Write);

            var pk0 = getRowResponse.PrimaryKey["PK0"];
            Console.WriteLine("PrimaryKey PK0 Value {0}", pk0.StringValue);
            var pk1 = getRowResponse.PrimaryKey["PK1"];
            Console.WriteLine("PrimaryKey PK1 Value {0}", pk1.IntegerValue);

            var row = getRowResponse.Row;

            foreach(Column column in row.GetColumn("Col0"))
            {
                Console.WriteLine(column);
            }

            Assert.AreEqual(2, row.GetColumn("Col0").Count);

            DeleteTable();
        }

        [Test]
        public void UpdateRowTest()
        {
            CreateTable();

            PutRow();

            var otsClient = OTSClient;
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var updateOfAttribute = new UpdateOfAttribute();
            updateOfAttribute.AddAttributeColumnToPut("NewColumn", new ColumnValue(123));
            updateOfAttribute.AddAttributeColumnToDelete("Col0");

            var updateRowRequest = new UpdateRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                updateOfAttribute);
            var updateRowResponse = otsClient.UpdateRow(updateRowRequest);

            Console.WriteLine("UpdateRow CU Consumed: Read {0} Write {1}",
                updateRowResponse.ConsumedCapacityUnit.Read,
                updateRowResponse.ConsumedCapacityUnit.Write);
            DeleteTable();
        }

        [Test]
        public void DeleteRowTest()
        {
            CreateTable();
            PutRow();

            var otsClient = OTSClient;

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var deleteRowRequest = new DeleteRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey);
            var deleteRowResponse = otsClient.DeleteRow(deleteRowRequest);
            Console.WriteLine("DeleteRow CU Consumed: Read {0} Write {1}",
                deleteRowResponse.ConsumedCapacityUnit.Read,
                deleteRowResponse.ConsumedCapacityUnit.Write);
            DeleteTable();
        }

        [Test]
        public void BatchWriteRowTest()
        {
            CreateTable();
            BatchWriteRow();
            DeleteTable();
        }

        [Test]
        public void BatchGetRowTest()
        {
            CreateTable();
            BatchWriteRow();
            var otsClient = OTSClient;

            var primaryKey1 = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", new ColumnValue(0) }
            };

            var primaryKey2 = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", new ColumnValue(1) }
            };

            var batchGetRowRequest = new BatchGetRowRequest();
            var primaryKeys = new List<PrimaryKey>() {
                primaryKey1,
                primaryKey2
            };

            batchGetRowRequest.Add(TestTableName, primaryKeys);

            //condition or maxVersions must be specified
            foreach(var criteria in batchGetRowRequest.GetCriterias())
            {
                criteria.MaxVersions = 1;
            }
            
            var batchGetRowRespnse = otsClient.BatchGetRow(batchGetRowRequest);

            foreach (var responseForOneTable in batchGetRowRespnse.RowDataGroupByTable)
            {
                foreach (var row in responseForOneTable.Value)
                {
                    Console.WriteLine(row.Row);
                }
            }

            DeleteTable();
        }

        [Test]
        public void GetRangeTest()
        {
            CreateTable();
            BatchWriteRow();
            var otsClient = OTSClient;
            // 指定范围读取数据
            var startPrimaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", ColumnValue.INF_MIN }
            };

            var endPrimaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", ColumnValue.INF_MAX }
            };

            var consumed = new CapacityUnit(0, 0);

            var request = new GetIteratorRequest(TestTableName, 
                                                 GetRangeDirection.Forward,
                                                 startPrimaryKey, 
                                                 endPrimaryKey,
                                                 consumed);
            request.QueryCriteria.MaxVersions = 1;

            var iterator = OTSClient.GetRangeIterator(request);

            foreach (var rowData in iterator)
            {
                Console.WriteLine(rowData);
            }

            DeleteTable();
        }

        private void PutRow()
        {
            var otsClient = OTSClient;
            var putRowRequest = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE)
            );

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute1 = new Column("Col0", new ColumnValue("Hangzhou"));
            var attribute2 = new Column("Col1", new ColumnValue(false));
            var attribute3 = new Column("Col2", new ColumnValue(12.5));

            putRowRequest.RowPutChange = new RowPutChange(TestTableName, primaryKey);
            putRowRequest.RowPutChange.AddColumn(attribute1);
            putRowRequest.RowPutChange.AddColumn(attribute2);
            putRowRequest.RowPutChange.AddColumn(attribute3);

            var putRowResponse = otsClient.PutRow(putRowRequest);
            Console.WriteLine("PutRow CU Consumed: Read {0} Write {1}",
                putRowResponse.ConsumedCapacityUnit.Read,
                putRowResponse.ConsumedCapacityUnit.Write);
        }

        private void BatchWriteRow()
        {
            var otsClient = OTSClient;

            var primaryKey1 = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", new ColumnValue(0) }
            };

            var attribute1 = new AttributeColumns
            {
                { "Col0", new ColumnValue("Hangzhou") }
            };

            var primaryKey2 = new PrimaryKey
            {
                { "PK0", new ColumnValue("TestData") },
                { "PK1", new ColumnValue(1) }
            };

            var attribute2 = new AttributeColumns
            {
                { "Col0", new ColumnValue("Shanghai") }
            };

            var rowChanges = new RowChanges(TestTableName);
            rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey1, attribute1);
            rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey2, attribute2);
            var batchWriteRowRequest = new BatchWriteRowRequest();
            batchWriteRowRequest.Add(TestTableName, rowChanges);

            var batchWriteRowResponse = otsClient.BatchWriteRow(batchWriteRowRequest);

            foreach (var responseForOneTable in batchWriteRowResponse.TableRespones)
            {
                foreach (var row in responseForOneTable.Value.Responses)
                {
                    Console.WriteLine(row);
                }
            }
        }

        private void CreateAutoIncrementColumnTable(PrimaryKeySchema schema)
        {
            var tableMeta = new TableMeta(TestTableName, schema);

            var tableOptions = new TableOptions
            {
                MaxVersions = 10,
                TimeToLive = -1
            };
            var reservedThroughput = new CapacityUnit(0, 0);

            var request = new CreateTableRequest(tableMeta, reservedThroughput)
            {
                TableOptions = tableOptions
            };

            OTSClient.CreateTable(request);

            WaitForTableReady();
        }
    }
}
