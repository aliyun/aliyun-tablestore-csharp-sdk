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
using System.Threading;
using System.Net;
using System.IO;
using System.Security.Cryptography;
using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.UnitTest
{
    class APITestContext
    {
        public string tableName;
        public PrimaryKeySchema pkSchema;
        public CapacityUnit reservedThroughput;
        public PrimaryKey primaryKey;
        public AttributeColumns attribute;
        public UpdateOfAttribute updateOfAttributeForPut;
        public UpdateOfAttribute updateOfAttributeForDelete;
        public Condition condition;
        public GetRangeDirection direction = GetRangeDirection.Forward;
        public PrimaryKey startPrimaryKey;
        public PrimaryKey endPrimaryKey;
        public HashSet<string> columnsToGet;
        public int? limit;
        public CapacityUnit putRowConsumed;
        public CapacityUnit getRowConsumed;
        public CapacityUnit updateRowConsumed;
        public CapacityUnit deleteRowConsumed;
        public CapacityUnit getRangeConsumed;
        public Dictionary<string, string> expectedFailure;
        public string allFailedMessage;
    }

    class LogToFileHandler : OTSDefaultLogHandler
    {
        public static new void DefaultErrorLogHandler(string message)
        {
            var dateString = GetDateTimeString();
            using (StreamWriter w = File.AppendText("/Users/xiaofeizhao/Downloads/log.txt"))
            {
                w.Write("OTSClient ERROR {0} {1}", dateString, message);
            }
        }

        public static new void DefaultDebugLogHandler(string message)
        {
            var dateString = GetDateTimeString();
            using (StreamWriter w = File.AppendText("/Users/xiaofeizhao/Downloads/log.txt"))
            {
                w.Write("OTSClient DEBUG {0} {1}", dateString, message);
            }
        }
    }

    class OTSUnitTestBase
    {
        public string TestEndPoint = Test.Config.Endpoint;
        public string TestAccessKeyID = Test.Config.AccessKeyId;
        public string TestAccessKeySecret = Test.Config.AccessKeySecret;
        public string TestInstanceName = Test.Config.InstanceName;
        public OTSClient OTSClient;

        // Predefined test data
        public static string TestTableName;
        public static PrimaryKey PrimaryKeyWith4Columns;
        public static PrimaryKey MinPrimaryKeyWith4Columns;
        public static PrimaryKey MaxPrimaryKeyWith4Columns;
        public static AttributeColumns AttributeWith5Columns;
        public static List<PrimaryKey> PrimaryKeyList;
        public static List<AttributeColumns> AttributeColumnsList;

        public APITestContext TestContext;

        [SetUp]
        public void Setup()
        {
            Thread.Sleep(1000);

            TestTableName = "SampleTestName";


            var clientConfig = new OTSClientConfig(
                                   TestEndPoint,
                                   TestAccessKeyID,
                                   TestAccessKeySecret,
                                   TestInstanceName
            );

            Console.WriteLine("Endpoint: {0}", TestEndPoint);
            Console.WriteLine("TestAccessKeyID: {0}", TestAccessKeyID);
            Console.WriteLine("TestAccessKeySecret: {0}", TestAccessKeySecret);
            Console.WriteLine("TestInstanceName: {0}", TestInstanceName);
            clientConfig.OTSDebugLogHandler = LogToFileHandler.DefaultDebugLogHandler;
            clientConfig.OTSErrorLogHandler = LogToFileHandler.DefaultErrorLogHandler;
            OTSClient = new OTSClient(clientConfig);
            OTSClientTestHelper.Reset();

            foreach (var tableName in OTSClient.ListTable(new ListTableRequest()).TableNames)
            {
                DeleteTable(tableName);
            }

            PrimaryKeyWith4Columns = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue("DEF") },
                { "PK2", new ColumnValue(123) },
                { "PK3", new ColumnValue(456) }
            };

            MinPrimaryKeyWith4Columns = new PrimaryKey
            {
                { "PK0", ColumnValue.INF_MIN },
                { "PK1", new ColumnValue("DEF") },
                { "PK2", new ColumnValue(123) },
                { "PK3", new ColumnValue(456) }
            };

            MaxPrimaryKeyWith4Columns = new PrimaryKey
            {
                { "PK0", ColumnValue.INF_MAX },
                { "PK1", new ColumnValue("DEF") },
                { "PK2", new ColumnValue(123) },
                { "PK3", new ColumnValue(456) }
            };

            AttributeWith5Columns = new AttributeColumns
            {
                { "Col0", new ColumnValue("ABC") },
                { "Col1", new ColumnValue(123) },
                { "Col2", new ColumnValue(3.14) },
                { "Col3", new ColumnValue(true) },
                { "Col4", new ColumnValue(new byte[] { 0x20, 0x20 }) }
            };

            PrimaryKeyList = new List<PrimaryKey>();
            AttributeColumnsList = new List<AttributeColumns>();


            for (int i = 0; i < 1000; i++)
            {
                PrimaryKeyList.Add(GetPredefinedPrimaryKeyWith4PK(i));
                AttributeColumnsList.Add(GetPredefinedAttributeWith5PK(i));
            }
        }

        public PrimaryKey GetPredefinedPrimaryKeyWith4PK(int index)
        {
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC" + index) },
                { "PK1", new ColumnValue("DEF" + index) },
                { "PK2", new ColumnValue(123 + index) },
                { "PK3", new ColumnValue(456 + index) }
            };
            return primaryKey;
        }

        public AttributeColumns GetPredefinedAttributeWith5PK(int index)
        {
            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("ABC" + index) },
                { "Col1", new ColumnValue(123 + index) },
                { "Col2", new ColumnValue(3.14 + index) },
                { "Col3", new ColumnValue(index % 2 == 0) },
                { "Col4", new ColumnValue(new byte[] { 0x20, 0x20 }) }
            };
            return attribute;
        }

        public static void WaitForTableReady()
        {
            Thread.Sleep(2 * 1000);
        }

        public static void AssertColumns(Dictionary<string, ColumnValue> expect, Dictionary<string, ColumnValue> actual, HashSet<string> columnsToGet = null)
        {
            if (columnsToGet != null && columnsToGet.Count != 0)
            {
                var expectReal = new Dictionary<string, ColumnValue>();

                foreach (var columnName in columnsToGet)
                {
                    if (expect.ContainsKey(columnName))
                    {
                        expectReal.Add(columnName, expect[columnName]);
                    }
                }

                expect = expectReal;
            }

            if (expect == null && actual == null)
            {
                return;
            }


            Assert.IsTrue(expect != null && actual != null, "expect and actual should NOT be null");

            Assert.AreEqual(expect.Count, actual.Count);

            foreach (var expectValue in expect)
            {
                Assert.IsTrue(actual.ContainsKey(expectValue.Key));
                var actualItem = actual[expectValue.Key];
                var expectItem = expectValue.Value;

                var result = expectItem.CompareTo(actualItem);

                Assert.IsTrue(result ==0, "columnValue Not equal, expect:" + expectItem + ", actual:" + actualItem);
            }
        }

        public static void AssertPrimaryKey(PrimaryKey expect, PrimaryKey actual, HashSet<string> columnsToGet = null)
        {
            AssertColumns(expect, actual, columnsToGet);
        }

        public static void AssertAttribute(AttributeColumns expect, AttributeColumns actual, HashSet<string> columnsToGet = null)
        {
            AssertColumns(expect, actual, columnsToGet);
        }

        public static void AssertAttribute(AttributeColumns expect, Column[] actual, HashSet<string> columnsToGet = null)
        {
            AssertColumns(expect, AttributeColumns.ParseColumnArray(actual), columnsToGet);
        }

        public static void AssertOTSServerException(OTSServerException expect, OTSServerException actual)
        {
            if (expect.APIName != actual.APIName ||
                expect.HttpStatusCode != actual.HttpStatusCode ||
                expect.ErrorCode != actual.ErrorCode ||
                expect.ErrorMessage != actual.ErrorMessage)
            {

                throw new AssertionException(String.Format(
                    "OTSServerException Assert Failed. expect: {0} actual {1}",
                    expect.Message, actual.Message
                ));
            }
        }

        public void CreateTestTable(string tableName, PrimaryKeySchema schema, CapacityUnit reservedThroughput, bool waitFlag = true)
        {
            var tableMeta = new TableMeta(tableName, schema);

            var tableOptions = new TableOptions
            {
                MaxVersions = 10,
                TimeToLive = -1
            };

            var request = new CreateTableRequest(tableMeta, reservedThroughput)
            {
                TableOptions = tableOptions
            };

            OTSClient.CreateTable(request);

            if (waitFlag)
            {
                WaitForTableReady();
            }
        }

        public void CreateTestTableWith2PK()
        {
            var schema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.Integer }
            };

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));
        }

        public void CreateTestTableWith4PK(CapacityUnit reservedThroughput = null, string tableName = null)
        {
            if (reservedThroughput == null)
            {
                reservedThroughput = new CapacityUnit(0, 0);
            }

            var schema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.String },
                { "PK2", ColumnValueType.Integer },
                { "PK3", ColumnValueType.Integer }
            };

            if (string.IsNullOrEmpty(tableName))
            {
                CreateTestTable(TestTableName, schema, reservedThroughput);
            }
            else
            {
                CreateTestTable(tableName, schema, reservedThroughput);
            }
        }

        public static void AssertCapacityUnit(CapacityUnit expect, CapacityUnit actual)
        {
            if (expect == null && actual == null)
            {
                return;
            }

            if (expect == null || actual == null)
            {
                Assert.Fail();
            }

            Assert.AreEqual(expect.Read, actual.Read, "CapacityUnit Read not Matched");
            Assert.AreEqual(expect.Write, actual.Write, "CapacityUnit Write not Matched");
        }

        /// <summary>
        /// Puts the single row.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="primaryKey">Primary key.</param>
        /// <param name="attributes">Attributes.</param>
        public void PutSingleRow(string tableName, PrimaryKey primaryKey, AttributeColumns attributes)
        {
            var request = new PutRowRequest(tableName, new Condition(RowExistenceExpectation.IGNORE));
            request.RowPutChange.PrimaryKey = primaryKey;
            foreach (var attribute in attributes)
            {
                request.RowPutChange.AddColumn(attribute.Key, attribute.Value);
            }

            OTSClient.PutRow(request);
        }

        public void CheckSingleRow(string tableName, PrimaryKey primaryKey,
                                   AttributeColumns attribute,
                                   CapacityUnit expectCapacityUnitConsumed = null,
                                   HashSet<string> columnsToGet = null,
                                   bool isEmpty = false,
                                   IColumnCondition condition = null)
        {
            var request = new GetRowRequest(tableName, primaryKey, columnsToGet, condition);

            var response = OTSClient.GetRow(request);

            PrimaryKey primaryKeyToExpect;
            AttributeColumns attributeToExpect;

            if (isEmpty)
            {
                primaryKeyToExpect = new PrimaryKey();
                attributeToExpect = new AttributeColumns();
            }
            else if (columnsToGet == null || columnsToGet.Count == 0)
            {
                primaryKeyToExpect = primaryKey;
                attributeToExpect = attribute;
            }
            else
            {
                primaryKeyToExpect = primaryKey;
                attributeToExpect = new AttributeColumns();
                foreach (var columnName in columnsToGet)
                {
                    if (attribute.ContainsKey(columnName))
                    {
                        attributeToExpect.Add(columnName, attribute[columnName]);
                    }
                }
            }

            AssertColumns(primaryKeyToExpect, response.PrimaryKey);
            AssertColumns(attributeToExpect, response.Attribute);

            if (expectCapacityUnitConsumed != null)
            {
                AssertCapacityUnit(expectCapacityUnitConsumed, response.ConsumedCapacityUnit);
            }
        }

        public static BatchWriteRowResponseForOneTable GetNewBatchWriteRowResponseForOneTable()
        {
            var item = new BatchWriteRowResponseForOneTable
            {
                Responses = new List<BatchWriteRowResponseItem>()
            };

            return item;
        }

        private void AssertOneOperationInBatchWriteRowResponse(
            IList<BatchWriteRowResponseItem> expect, IList<BatchWriteRowResponseItem> actual)
        {
            Assert.AreEqual(expect.Count, actual.Count);

            for (int i = 0; i < expect.Count; i++)
            {
                var expectItem = expect[i];
                var actualItem = actual[i];

                Assert.AreEqual(expectItem.IsOK, actualItem.IsOK);
                Assert.AreEqual(expectItem.ErrorCode, actualItem.ErrorCode);
                Assert.AreEqual(expectItem.ErrorMessage, actualItem.ErrorMessage);
                Assert.AreEqual(expectItem.TableName, actualItem.TableName);
                Assert.AreEqual(expectItem.Index, actualItem.Index);
                AssertCapacityUnit(expectItem.Consumed, expectItem.Consumed);
            }
        }

        public void AssertBatchWriteRowResponse(
            BatchWriteRowResponse expect, BatchWriteRowResponse actual)
        {
            Assert.AreEqual(expect.TableRespones.Keys, actual.TableRespones.Keys);

            foreach (var table in expect.TableRespones)
            {
                Assert.IsTrue(actual.TableRespones.ContainsKey(table.Key));
                var tableExpect = table.Value;
                var tableActual = actual.TableRespones[table.Key];
                AssertOneOperationInBatchWriteRowResponse(tableExpect.Responses, tableActual.Responses);
            }
        }

        public void AssertBatchGetRowResponse(
            BatchGetRowResponse expect, BatchGetRowResponse actual)
        {
            Assert.AreEqual(expect.RowDataGroupByTable.Keys, actual.RowDataGroupByTable.Keys);

            foreach (var table in expect.RowDataGroupByTable)
            {
                Assert.IsTrue(actual.RowDataGroupByTable.ContainsKey(table.Key));
                var tableExpect = table.Value;
                var tableActual = actual.RowDataGroupByTable[table.Key];
                Assert.AreEqual(tableExpect.Count, tableActual.Count);
                for (int i = 0; i < tableExpect.Count; i++)
                {
                    var expectItem = tableExpect[i];
                    var actualItem = tableActual[i];

                    Assert.AreEqual(expectItem.IsOK, actualItem.IsOK);
                    Assert.AreEqual(expectItem.ErrorCode, actualItem.ErrorCode);
                    Assert.AreEqual(expectItem.ErrorMessage, actualItem.ErrorMessage);
                    AssertCapacityUnit(expectItem.Consumed, expectItem.Consumed);

                    AssertColumns(
                        expectItem.PrimaryKey,
                        actualItem.PrimaryKey);
                    AssertColumns(
                        expectItem.Attribute,
                        actualItem.Attribute);
                }
            }
        }

        public void PutSinglePredefinedRow(int index)
        {
            PutSingleRow(TestTableName,
                         GetPredefinedPrimaryKeyWith4PK(index),
                         GetPredefinedAttributeWith5PK(index));
        }

        public void AssertGetRangeRowWithPredefinedRow(Row row, int index)
        {
            AssertPrimaryKey(GetPredefinedPrimaryKeyWith4PK(index), row.GetPrimaryKey());
            //AssertAttribute(GetPredefinedAttributeWith5PK(index), row.GetColumns());
        }

        public void AssertPrimaryKeySchema(PrimaryKeySchema expect, PrimaryKeySchema actual)
        {
            Assert.AreEqual(expect.Count, actual.Count);

            for (int i = 0; i < expect.Count; i++)
            {
                Assert.AreEqual(expect[i].Item1, actual[i].Item1);
                Assert.AreEqual(expect[i].Item2, actual[i].Item2);
            }
        }

        public void PutPredefinedRows(int count)
        {
            int index = 0;
            while (count > 0)
            {
                var rowChanges = new RowChanges(TestTableName);

                for (int i = 0; i < (count > 100 ? 100 : count); i++)
                {
                    rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE),
                        GetPredefinedPrimaryKeyWith4PK(index + i),
                        GetPredefinedAttributeWith5PK(index + i)
                    );
                }

                var request = new BatchWriteRowRequest();
                request.Add(TestTableName, rowChanges);
                OTSClient.BatchWriteRow(request);

                count -= 100;
                index += 100;
            }
        }


        public void SetTestConext(string tableName = null,
            PrimaryKeySchema pkSchema = null,
            CapacityUnit reservedThroughput = null,
            PrimaryKey primaryKey = null,
            AttributeColumns attribute = null,
            UpdateOfAttribute updateOfAttributeForPut = null,
            UpdateOfAttribute updateOfAttributeForDelete = null,
            Condition condition = null,
            GetRangeDirection direction = GetRangeDirection.Forward,
            PrimaryKey startPrimaryKey = null,
            PrimaryKey endPrimaryKey = null,
            HashSet<string> columnsToGet = null,
            int? limit = null,
            CapacityUnit putRowConsumed = null,
            CapacityUnit getRowConsumed = null,
            CapacityUnit updateRowConsumed = null,
            CapacityUnit deleteRowConsumed = null,
            CapacityUnit getRangeConsumed = null,
            Dictionary<string, string> expectedFailure = null,
            string allFailedMessage = null)
        {

            var DefaultPrimaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.String },
                { "PK2", ColumnValueType.Integer },
                { "PK3", ColumnValueType.Integer }
            };

            var DefaultReservedThroughput = new CapacityUnit(0, 0);

            TestContext = new APITestContext
            {
                expectedFailure = expectedFailure,
                allFailedMessage = allFailedMessage,
                tableName = tableName ?? OTSUnitTestBase.TestTableName,
                pkSchema = pkSchema ?? DefaultPrimaryKeySchema,
                reservedThroughput = reservedThroughput ?? DefaultReservedThroughput,
                primaryKey = primaryKey ?? PrimaryKeyWith4Columns,
                attribute = attribute ?? AttributeWith5Columns,
                condition = condition ?? new Condition(RowExistenceExpectation.IGNORE),
                startPrimaryKey = startPrimaryKey ?? MinPrimaryKeyWith4Columns,
                endPrimaryKey = endPrimaryKey ?? MaxPrimaryKeyWith4Columns,
                putRowConsumed = putRowConsumed ?? new CapacityUnit(0, 1),
                getRowConsumed = getRowConsumed ?? new CapacityUnit(1, 0),
                updateRowConsumed = updateRowConsumed ?? GetDefaultCapacityUnit(condition),
                deleteRowConsumed = deleteRowConsumed ?? GetDefaultCapacityUnit(condition),
                getRangeConsumed = getRangeConsumed ?? new CapacityUnit(1, 0),
                columnsToGet = columnsToGet,
                limit = limit,
                direction = direction
            };

            if (updateOfAttributeForPut == null)
            {
                updateOfAttributeForPut = new UpdateOfAttribute();
                foreach (var item in TestContext.attribute)
                {
                    updateOfAttributeForPut.AddAttributeColumnToPut(item.Key, item.Value);
                }
            }

            if (updateOfAttributeForDelete == null)
            {
                updateOfAttributeForDelete = new UpdateOfAttribute();
                foreach (var item in TestContext.attribute)
                {
                    updateOfAttributeForDelete.AddAttributeColumnToDelete(item.Key);
                }
            }

            TestContext.updateOfAttributeForPut = updateOfAttributeForPut;
            TestContext.updateOfAttributeForDelete = updateOfAttributeForDelete;
        }

        /// <summary>
        /// Gets the default capacity unit for delete and update operation
        /// </summary>
        /// <returns>The default capacity unit.</returns>
        /// <param name="condition">Condition.</param>
        private CapacityUnit GetDefaultCapacityUnit(Condition condition)
        {
            if(condition == null)
            {
                return new CapacityUnit(0, 1);
            }

            switch(condition.RowExistenceExpect)
            {
                case RowExistenceExpectation.IGNORE:
                    return new CapacityUnit(0, 1);
                case RowExistenceExpectation.EXPECT_EXIST:
                case RowExistenceExpectation.EXPECT_NOT_EXIST:
                    return new CapacityUnit(1, 1);
                default:
                    throw new Exception("not support default RowExistenceExpect: " + condition.RowExistenceExpect);
            }
        }

        public void TestAPIWithParameter(string apiName)
        {
            var tableName = TestContext.tableName;
            var pkSchema = TestContext.pkSchema;
            var reservedThroughput = TestContext.reservedThroughput;
            var primaryKey = TestContext.primaryKey;
            var attribute = TestContext.attribute;
            var condition = TestContext.condition;
            var startPrimaryKey = TestContext.startPrimaryKey;
            var endPrimaryKey = TestContext.endPrimaryKey;
            var putRowConsumed = TestContext.putRowConsumed;
            var getRowConsumed = TestContext.getRowConsumed;
            var updateRowConsumed = TestContext.updateRowConsumed;
            var deleteRowConsumed = TestContext.deleteRowConsumed;
            var getRangeConsumed = TestContext.getRangeConsumed;
            var attributeForPut = TestContext.updateOfAttributeForPut;
            var attributeForDelete = TestContext.updateOfAttributeForDelete;
            var columnsToGet = TestContext.columnsToGet;
            var limit = TestContext.limit;
            var direction = TestContext.direction;

            var tableMeta = new TableMeta(tableName, pkSchema);

            switch (apiName)
            {
                case "CreateTable":
                    var request0 = new CreateTableRequest(tableMeta, reservedThroughput);
                    OTSClient.CreateTable(request0);
                    return;

                case "ListTable":
                    var request1 = new ListTableRequest();
                    var response1 = OTSClient.ListTable(request1);
                    Assert.AreEqual(new List<string>() { tableName }, response1.TableNames);
                    return;

                case "UpdateTable":
                    var request2 = new UpdateTableRequest(tableName)
                    {
                        ReservedThroughput = reservedThroughput
                    };
                    var response2 = OTSClient.UpdateTable(request2);

                    if (reservedThroughput.Read.HasValue && reservedThroughput.Write.HasValue)
                    {
                        AssertCapacityUnit(
                            reservedThroughput,
                            response2.ReservedThroughputDetails.CapacityUnit);
                    }

                    Assert.IsTrue(response2.ReservedThroughputDetails.LastDecreaseTime >= 0);
                    Assert.IsTrue(response2.ReservedThroughputDetails.LastIncreaseTime >= 0);
                    Assert.IsTrue(response2.ReservedThroughputDetails.NumberOfDecreasesToday >= 0);
                    return;

                case "DeleteTable":
                    var request3 = new DeleteTableRequest(tableName);
                    OTSClient.DeleteTable(request3);

                    var request31 = new ListTableRequest();
                    var response31 = OTSClient.ListTable(request31);
                    Assert.AreEqual(new List<string>() { }, response31.TableNames);
                    return;

                case "DescribeTable":
                    var request4 = new DescribeTableRequest(tableName);
                    var response4 = OTSClient.DescribeTable(request4);
                    Assert.AreEqual(tableName, response4.TableMeta.TableName);
                    AssertPrimaryKeySchema(pkSchema, response4.TableMeta.PrimaryKeySchema);
                    AssertCapacityUnit(reservedThroughput, response4.ReservedThroughputDetails.CapacityUnit);
                    Assert.IsTrue(response4.ReservedThroughputDetails.LastDecreaseTime >= 0);
                    Assert.IsTrue(response4.ReservedThroughputDetails.LastIncreaseTime >= 0);
                    Assert.IsTrue(response4.ReservedThroughputDetails.NumberOfDecreasesToday >= 0);
                    return;

                case "PutRow":
                    var request5 = new PutRowRequest(tableName, condition)
                    {
                        RowPutChange = new RowPutChange(tableName, primaryKey)
                    };

                    foreach(var attr in attributeForPut.AttributeColumnsToPut)
                    {
                        request5.RowPutChange.AddColumn(new Column(attr.Key, attr.Value));
                    }

                    var response5 = OTSClient.PutRow(request5);
                    AssertCapacityUnit(putRowConsumed, response5.ConsumedCapacityUnit);
                    return;

                case "GetRow":
                    var request6 = new GetRowRequest(tableName, primaryKey, columnsToGet);
                    var response6 = OTSClient.GetRow(request6);
                    AssertPrimaryKey(primaryKey, response6.PrimaryKey, columnsToGet);
                    AssertAttribute(attribute, response6.Attribute, columnsToGet);
                    AssertCapacityUnit(getRowConsumed, response6.ConsumedCapacityUnit);
                    return;

                case "DeleteRow":
                    var request7 = new DeleteRowRequest(tableName, condition, primaryKey);
                    var response7 = OTSClient.DeleteRow(request7);
                    AssertCapacityUnit(deleteRowConsumed, response7.ConsumedCapacityUnit);

                    var request71 = new GetRowRequest(tableName, primaryKey);
                    var response71 = OTSClient.GetRow(request71);
                    AssertPrimaryKey(new PrimaryKey(), response71.PrimaryKey);
                    AssertAttribute(new AttributeColumns(), response71.Attribute);
                    return;

                case "UpdateRow_Put":
                    var request8 = new UpdateRowRequest(tableName, condition, primaryKey, attributeForPut);
                    var response8 = OTSClient.UpdateRow(request8);
                    AssertCapacityUnit(updateRowConsumed, response8.ConsumedCapacityUnit);

                    var request81 = new GetRowRequest(tableName, primaryKey);
                    var response81 = OTSClient.GetRow(request81);
                    AssertPrimaryKey(primaryKey, response81.PrimaryKey);
                    AssertAttribute(attribute, response81.Attribute);
                    AssertCapacityUnit(getRowConsumed, response81.ConsumedCapacityUnit);

                    return;

                case "UpdateRow_Delete":
                    var request9 = new UpdateRowRequest(tableName, condition, primaryKey, attributeForDelete);
                    var response9 = OTSClient.UpdateRow(request9);
                    AssertCapacityUnit(deleteRowConsumed, response9.ConsumedCapacityUnit);

                    var request91 = new GetRowRequest(tableName, primaryKey);
                    var response91 = OTSClient.GetRow(request91);
                    // Don't assert primary key
                    AssertAttribute(new AttributeColumns(), response91.Attribute);
                    return;

                case "BatchGetRow":
                    var request11 = new BatchGetRowRequest();
                    request11.Add(tableName, new List<PrimaryKey>() { primaryKey }, columnsToGet);
                    var response11 = OTSClient.BatchGetRow(request11);
                    Assert.AreEqual(1, response11.RowDataGroupByTable.Count);
                    Assert.IsTrue(response11.RowDataGroupByTable.ContainsKey(tableName));
                    Assert.AreEqual(1, response11.RowDataGroupByTable[tableName].Count);

                    if (!response11.RowDataGroupByTable[tableName][0].IsOK)
                    {
                        throw new OTSServerException(apiName, HttpStatusCode.OK,
                                                     response11.RowDataGroupByTable[tableName][0].ErrorCode,
                                                     response11.RowDataGroupByTable[tableName][0].ErrorMessage);
                    }
                    AssertPrimaryKey(primaryKey, response11.RowDataGroupByTable[tableName][0].PrimaryKey);
                    AssertAttribute(attribute, response11.RowDataGroupByTable[tableName][0].Attribute);
                    AssertCapacityUnit(getRowConsumed, response11.RowDataGroupByTable[tableName][0].Consumed);
                    return;

                case "BatchWriteRow_Put":
                    var request12 = new BatchWriteRowRequest();
                    var rowChanges = new RowChanges(tableName);
                    rowChanges.AddPut(condition, primaryKey, attribute);
                    request12.Add(tableName, rowChanges);
                    var response12 = OTSClient.BatchWriteRow(request12);
                    Assert.AreEqual(1, response12.TableRespones.Count);
                    Assert.IsTrue(response12.TableRespones.ContainsKey(tableName));
                    Assert.AreEqual(1, response12.TableRespones[tableName].Responses.Count);
                    if (response12.TableRespones[tableName].Responses[0].IsOK)
                    {
                        AssertCapacityUnit(putRowConsumed, response12.TableRespones[tableName].Responses[0].Consumed);
                    }
                    else
                    {
                        throw new OTSServerException("/BatchWriteRow", HttpStatusCode.OK,
                                                     response12.TableRespones[tableName].Responses[0].ErrorCode,
                                                     response12.TableRespones[tableName].Responses[0].ErrorMessage);
                    }


                    var request121 = new GetRowRequest(tableName, primaryKey);
                    var response121 = OTSClient.GetRow(request121);
                    AssertPrimaryKey(primaryKey, response121.PrimaryKey);
                    AssertAttribute(attribute, response121.Attribute);
                    AssertCapacityUnit(getRowConsumed, response121.ConsumedCapacityUnit);
                    return;

                case "BatchWriteRow_Update":
                    var request13 = new BatchWriteRowRequest();
                    var rowChanges2 = new RowChanges(tableName);
                    rowChanges2.AddUpdate(condition, primaryKey, attributeForPut);
                    request13.Add(tableName, rowChanges2);
                    var response13 = OTSClient.BatchWriteRow(request13);
                    Assert.AreEqual(1, response13.TableRespones.Count);
                    Assert.IsTrue(response13.TableRespones.ContainsKey(tableName));
                    Assert.AreEqual(1, response13.TableRespones[tableName].Responses.Count);
                    if (response13.TableRespones[tableName].Responses[0].IsOK)
                    {
                        AssertCapacityUnit(updateRowConsumed, response13.TableRespones[tableName].Responses[0].Consumed);
                    }
                    else
                    {
                        throw new OTSServerException("/BatchWriteRow", HttpStatusCode.OK,
                                                     response13.TableRespones[tableName].Responses[0].ErrorCode,
                                                     response13.TableRespones[tableName].Responses[0].ErrorMessage);
                    }

                    var request131 = new GetRowRequest(tableName, primaryKey);
                    var response131 = OTSClient.GetRow(request131);
                    AssertPrimaryKey(primaryKey, response131.PrimaryKey);
                    AssertAttribute(attribute, response131.Attribute);
                    AssertCapacityUnit(getRowConsumed, response131.ConsumedCapacityUnit);
                    return;

                case "BatchWriteRow_Delete":
                    var request14 = new BatchWriteRowRequest();
                    var rowChanges3 = new RowChanges(tableName);
                    rowChanges3.AddDelete(condition, primaryKey);
                    request14.Add(tableName, rowChanges3);
                    var response14 = OTSClient.BatchWriteRow(request14);
                    Assert.AreEqual(1, response14.TableRespones.Count);
                    Assert.IsTrue(response14.TableRespones.ContainsKey(tableName));
                    Assert.AreEqual(1, response14.TableRespones[tableName].Responses.Count);

                    if (response14.TableRespones[tableName].Responses[0].IsOK)
                    {
                        AssertCapacityUnit(deleteRowConsumed,
                            response14.TableRespones[tableName].Responses[0].Consumed);
                    }
                    else
                    {
                        throw new OTSServerException("/BatchWriteRow", HttpStatusCode.OK,
                                                     response14.TableRespones[tableName].Responses[0].ErrorCode,
                                                     response14.TableRespones[tableName].Responses[0].ErrorMessage);
                    }
                    var request141 = new GetRowRequest(tableName, primaryKey);
                    var response141 = OTSClient.GetRow(request141);
                    AssertPrimaryKey(new PrimaryKey(), response141.PrimaryKey);
                    AssertAttribute(new AttributeColumns(), response141.Attribute);
                    return;

                case "GetRange":
                    var request15 = new GetRangeRequest(tableName, direction,
                                                       startPrimaryKey, endPrimaryKey,
                                                       columnsToGet, limit);
                    var response15 = OTSClient.GetRange(request15);
                    Assert.AreEqual(1, response15.RowDataList.Count);
                    Assert.AreEqual(null, response15.NextPrimaryKey);
                    AssertCapacityUnit(getRangeConsumed, response15.ConsumedCapacityUnit);
                    AssertPrimaryKey(primaryKey, response15.RowDataList[0].GetPrimaryKey(), columnsToGet);
                    //AssertAttribute(attribute, response15.RowDataList[0].Attribute, columnsToGet);
                    return;

                default:
                    throw new Exception(String.Format("invalid api name: {0}", apiName));
            }
        }

        public void TestSingleAPI(string apiName)
        {
            Console.WriteLine("Testing " + apiName);
            string failedMessage = TestContext.allFailedMessage;
            if (TestContext.expectedFailure != null &&
                TestContext.expectedFailure.ContainsKey(apiName))
            {
                failedMessage = TestContext.expectedFailure[apiName];
            }

            if (failedMessage != null)
            {
                try
                {
                    TestAPIWithParameter(apiName);
                    Assert.Fail();
                }
                catch (OTSServerException exception)
                {
                    Assert.AreEqual(failedMessage, exception.ErrorMessage);
                }
                catch (IOException exception)
                {
                    Assert.AreEqual(failedMessage, exception.Message);
                }
                catch(Exception exception)
                {
                    Console.WriteLine(exception.Message);
                }
            }
            else
            {
                TestAPIWithParameter(apiName);
            }
        }

        public void WaitBeforeUpdateTable()
        {
            Thread.Sleep(5000);
        }

        public void TestAllAPIWithTableName()
        {
            TestSingleAPI("CreateTable");
            WaitForTableReady();
            TestSingleAPI("DescribeTable");

            WaitBeforeUpdateTable();
            TestSingleAPI("UpdateTable");

            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");
            TestSingleAPI("GetRange");
            TestSingleAPI("BatchGetRow");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("UpdateRow_Delete");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("BatchWriteRow_Delete");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("BatchWriteRow_Update");

            TestSingleAPI("DeleteTable");
        }

        public void TestAllDataAPI(bool createTable = true, bool deleteTable = true)
        {
            if (createTable)
            {
                TestSingleAPI("CreateTable");
                WaitForTableReady();
            }

            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");
            TestSingleAPI("GetRange");
            TestSingleAPI("BatchGetRow");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("UpdateRow_Delete");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("BatchWriteRow_Delete");
            TestSingleAPI("DeleteRow");
            TestSingleAPI("BatchWriteRow_Update");

            if (createTable && deleteTable)
            {
                TestSingleAPI("DeleteTable");
            }
        }

        public void TestAllDataAPIWithAttribute(bool createTable = true)
        {
            if (createTable)
            {
                TestSingleAPI("CreateTable");
                WaitForTableReady();
            }
            TestSingleAPI("PutRow");
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("UpdateRow_Delete");
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("BatchWriteRow_Update");
            if (createTable)
            {
                TestSingleAPI("DeleteTable");
            }
        }

        public void TestAllDataAPIWithColumnValue(bool createTable = true)
        {
            if (createTable)
            {
                TestSingleAPI("CreateTable");
                WaitForTableReady();
            }
            TestSingleAPI("PutRow");
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("BatchWriteRow_Update");

            if (createTable)
            {
                TestSingleAPI("DeleteTable");
            }
        }

        public void TestAllDataAPIWithColumnsToGet()
        {
            TestSingleAPI("GetRow");
            TestSingleAPI("GetRange");
            TestSingleAPI("BatchGetRow");
        }

        public string MakeSignature(string apiName, Dictionary<string, string> headers, string accessKeySecret)
        {
            List<string> items = new List<string>();

            foreach (var item in headers)
            {
                if (item.Key.StartsWith("x-ots-", StringComparison.Ordinal) && item.Key != "x-ots-signature")
                {
                    items.Add(String.Format("{0}:{1}", item.Key, item.Value));
                }
            }

            items.Sort();
            var headerString = String.Join("\n", items);
            string signatureString = headerString + "\n" + apiName;
            var hmac = new HMACSHA1(System.Text.Encoding.ASCII.GetBytes(accessKeySecret));
            byte[] hashValue = hmac.ComputeHash(System.Text.Encoding.ASCII.GetBytes(signatureString));
            string signature = System.Convert.ToBase64String(hashValue);
            return signature;
        }

        public Dictionary<string, string>
            MakeResponseHeaders(string apiName, byte[] httpBody,
                                string accessKeyID = null,
                                string accessKeySecret = null,
                                bool hasRequestID = true,
                                bool hasContentMd5 = true)
        {
            accessKeyID = accessKeyID ?? TestAccessKeyID;
            accessKeySecret = accessKeySecret ?? TestAccessKeySecret;
            var serverTime = DateTime.UtcNow;
            var headers = new Dictionary<string, string>();

            // response md5
            if (hasContentMd5)
            {
                var md5hash = MD5.Create();
                byte[] hashData = md5hash.ComputeHash(httpBody);
                string contentMD5 = System.Convert.ToBase64String(hashData);
                headers.Add("x-ots-contentmd5", contentMD5);
            }

            // request id
            if (hasRequestID)
            {
                headers.Add("x-ots-requestid", "fake-request-id-for-test");
            }

            // date
            headers.Add("x-ots-date", Util.OtsUtils.FormatDateTimeStr(serverTime));

            // content type
            headers.Add("x-ots-contenttype", "blah");

            // authorization
            var signature = MakeSignature(apiName, headers, accessKeySecret);
            headers.Add("Authorization", String.Format("OTS {0}:{1}", accessKeyID, signature));

            return headers;
        }

        public byte[] MakeErrorPB(string errorCode, string errorMessage)
        {
            var builder = PB.Error.CreateBuilder();
            builder.Code = errorCode;
            builder.Message = errorMessage;
            var message = builder.Build();
            return message.ToByteArray();
        }

        public byte[] MakeListTableResponseBody()
        {
            var builder = PB.ListTableResponse.CreateBuilder();
            builder.AddTableNames("table1");
            builder.AddTableNames("table2");
            var message = builder.Build();
            return message.ToByteArray();
        }

        public void DeleteTable(string tableName = null)
        {
            if (tableName == null)
            {
                tableName = TestTableName;
            }

            var otsClient = OTSClient;
            var deleteTableRequest = new DeleteTableRequest(tableName);
            otsClient.DeleteTable(deleteTableRequest);
        }

        public void CreateTable(string tableName = null, PrimaryKeySchema primaryKeySchema = null)
        {
            var otsClient = OTSClient;

            if (tableName == null)
            {
                tableName = TestTableName;
            }

            if (primaryKeySchema == null)
            {
                primaryKeySchema = new PrimaryKeySchema
                {
                    { "PK0", ColumnValueType.String },
                    { "PK1", ColumnValueType.Integer }
                };
            }

            var reservedThroughput = new CapacityUnit(0, 0);

            CreateTestTable(tableName, primaryKeySchema, reservedThroughput);
        }
    }
}
