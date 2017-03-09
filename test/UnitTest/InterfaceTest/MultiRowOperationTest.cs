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
using System.Net;

using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.UnitTest.InterfaceTest
{

    [TestFixture]
    class MultiRowOperationTest : OTSUnitTestBase
    {
        #region BatchWriteRowTest
        /// <summary>
        /// BatchWriteRow没有包含任何表的情况。
        /// </summary>
        [Test]
        public void TestEmptyBatchWriteRow() 
        {
            var request = new BatchWriteRowRequest();
            
            try {
                OTSClient.BatchWriteRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchWriteRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "No row specified in the request of BatchWriteRow."
                ), exception);
            }
        }

        /// <summary>
        /// BatchWriteRow包含2个表，其中有1个表有1行，另外一个表为空的情况。
        /// </summary>
        [Test]
        public void TestEmptyTableInBatchWriteRow() 
        {
            var request = new BatchWriteRowRequest();
            var rowChange1 = new RowChanges();
            rowChange1.AddPut(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyWith4Columns, AttributeWith5Columns);
            request.Add("Table1", rowChange1);
            request.Add("Table2", new RowChanges());
            
            try {
                OTSClient.BatchWriteRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchWriteRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "No row specified in table: 'Table2'."
                ), exception);
            }
        }

        /// <summary>
        /// BatchWriteRow包含4个Put操作。
        /// </summary>
        [Test]
        public void TestPutOnlyInBatchWriteRow() 
        {
            CreateTestTableWith4PK();
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            request.Add(TestTableName, rowChange);
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            var item = GetNewBatchWriteRowResponseForOneTable();
            
            for (int i = 0; i < 4; i ++) {
                item.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
            }
            expectResponse.TableRespones.Add(TestTableName, item);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow(TestTableName, PrimaryKeyList[i], AttributeColumnsList[i]);
            }
        }

        /// <summary>
        /// BatchWriteRow包含4个Update操作。
        /// </summary>
        [Test]
        public void TestUpdateOnlyInBatchWriteRow() 
        {
            CreateTestTableWith4PK();
            
            for (int i = 0; i < 4; i ++) {
                PutSingleRow(TestTableName, PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            
            for (int i = 0; i < 4; i ++) {
                var update = new UpdateOfAttribute();
                
                if ( i % 2 == 0) {
                    update.AddAttributeColumnToPut("Col0", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col1", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col2", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col3", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col4", new ColumnValue(123));
                } else {
                    update.AddAttributeColumnToDelete("Col0");
                    update.AddAttributeColumnToDelete("Col1");
                    update.AddAttributeColumnToDelete("Col2");
                    update.AddAttributeColumnToDelete("Col3");
                    update.AddAttributeColumnToDelete("Col4");
                }
                
                rowChange.AddUpdate(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i], update);
            }
            
            request.Add(TestTableName, rowChange);
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            var item = GetNewBatchWriteRowResponseForOneTable();
            
            for (int i = 0; i < 4; i ++) {
                item.UpdateResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
            }
            expectResponse.TableRespones.Add(TestTableName, item);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                
                var attributeToExpect = new AttributeColumns();
                if (i % 2 == 0) {
                    attributeToExpect.Add("Col0", new ColumnValue(123));
                    attributeToExpect.Add("Col1", new ColumnValue(123));
                    attributeToExpect.Add("Col2", new ColumnValue(123));
                    attributeToExpect.Add("Col3", new ColumnValue(123));
                    attributeToExpect.Add("Col4", new ColumnValue(123));
                }
                
                CheckSingleRow(TestTableName, PrimaryKeyList[i], attributeToExpect);
            }
        }

        /// <summary>
        /// BatchWriteRow包含4个Delete操作。
        /// </summary>
        [Test]
        public void TestDeleteOnlyInBatchWriteRow() 
        {
            CreateTestTableWith4PK();
            
            for (int i = 0; i < 4; i ++) {
                PutSingleRow(TestTableName, PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            
            for (int i = 0; i < 4; i ++) {
                rowChange.AddDelete(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i]);
            }
            
            request.Add(TestTableName, rowChange);
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            var item = GetNewBatchWriteRowResponseForOneTable();
            
            for (int i = 0; i < 4; i ++) {
                item.DeleteResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
            }
            expectResponse.TableRespones.Add(TestTableName, item);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow(TestTableName, PrimaryKeyList[i], new AttributeColumns(), isEmpty : true);
            }
        }

        //// <summary>
        //// BatchWriteRow同时包含4个Put，4个Update和4个Delete操作。
        //// </summary>
        [Test]
        public void Test4PutUpdateDeleteInBatchWriteRow() 
        {
            CreateTestTableWith4PK();
            
            for (int i = 0; i < 4; i ++) {
                PutSingleRow(TestTableName, PrimaryKeyList[i + 4], AttributeColumnsList[i + 4]);
                PutSingleRow(TestTableName, PrimaryKeyList[i + 8], AttributeColumnsList[i + 8]);
            }
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i], AttributeColumnsList[i]);
                var update = new UpdateOfAttribute();
                
                if ( i % 2 == 0) {
                    update.AddAttributeColumnToPut("Col0", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col1", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col2", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col3", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col4", new ColumnValue(123));
                } else {
                    update.AddAttributeColumnToDelete("Col0");
                    update.AddAttributeColumnToDelete("Col1");
                    update.AddAttributeColumnToDelete("Col2");
                    update.AddAttributeColumnToDelete("Col3");
                    update.AddAttributeColumnToDelete("Col4");
                }
                
                rowChange.AddUpdate(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i + 4], update);
                
                rowChange.AddDelete(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i + 8]);
            }
            
            request.Add(TestTableName, rowChange);
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            var item = GetNewBatchWriteRowResponseForOneTable();
            
            for (int i = 0; i < 4; i ++) {
                item.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
                item.UpdateResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
                item.DeleteResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), TestTableName, i));
            }
            expectResponse.TableRespones.Add(TestTableName, item);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow(TestTableName, PrimaryKeyList[i], AttributeColumnsList[i]);
                
                var attributeToExpect = new AttributeColumns();
                if (i % 2 == 0) {
                    attributeToExpect.Add("Col0", new ColumnValue(123));
                    attributeToExpect.Add("Col1", new ColumnValue(123));
                    attributeToExpect.Add("Col2", new ColumnValue(123));
                    attributeToExpect.Add("Col3", new ColumnValue(123));
                    attributeToExpect.Add("Col4", new ColumnValue(123));
                }
                
                CheckSingleRow(TestTableName, PrimaryKeyList[i + 4], attributeToExpect);
                CheckSingleRow(TestTableName, PrimaryKeyList[i + 8], new AttributeColumns(), isEmpty : true);
            }

        }

        /// <summary>
        /// BatchWriteRow同时包含1000个Put，1000个Update和1000个Delete操作，期望返回服务端错误？
        /// </summary>
        [Test]
        public void Test1000PutUpdateDeleteInBatchWriteRow() 
        {                        
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            
            var primaryKeyList = new List<PrimaryKey>();
            var attributeList = new List<AttributeColumns>();
            
            for (int i = 0; i < 3000; i ++)
            {
                var primaryKey = new PrimaryKey();
                primaryKey.Add("PK0", new ColumnValue("ABC" + i));
                primaryKey.Add("PK1", new ColumnValue("DEF" + i));
                primaryKey.Add("PK2", new ColumnValue(123 + i));
                primaryKey.Add("PK3", new ColumnValue(456 + i));
                primaryKeyList.Add(primaryKey);
                
                var attribute = new AttributeColumns();
                attribute.Add("Col0", new ColumnValue("ABC" + i));
                attribute.Add("Col1", new ColumnValue(123 + i));
                attribute.Add("Col2", new ColumnValue(3.14 + i));
                attribute.Add("Col3", new ColumnValue(i % 2 == 0));
                attribute.Add("Col4", new ColumnValue(new byte[]{0x20, 0x20}));
                attributeList.Add(attribute);
            }
            for (int i = 0; i < 1000; i ++) {
                
                rowChange.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKeyList[i], attributeList[i]);
                var update = new UpdateOfAttribute();
                
                if ( i % 2 == 0) {
                    update.AddAttributeColumnToPut("Col0", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col1", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col2", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col3", new ColumnValue(123));
                    update.AddAttributeColumnToPut("Col4", new ColumnValue(123));
                } else {
                    update.AddAttributeColumnToDelete("Col0");
                    update.AddAttributeColumnToDelete("Col1");
                    update.AddAttributeColumnToDelete("Col2");
                    update.AddAttributeColumnToDelete("Col3");
                    update.AddAttributeColumnToDelete("Col4");
                }
                
                rowChange.AddUpdate(new Condition(RowExistenceExpectation.IGNORE), primaryKeyList[i + 1000], update);
                rowChange.AddDelete(new Condition(RowExistenceExpectation.IGNORE), primaryKeyList[i + 2000]);
            }
            
            request.Add(TestTableName, rowChange);
                        
            try {
                var response = OTSClient.BatchWriteRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchWriteRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "Rows count exceeds the upper limit"
                ), exception);
            }
        }

        /// <summary>
        /// BatchWriteRow包含4个表的情况。
        /// </summary>
        [Test]
        public void Test4TablesInBatchWriteRow() 
        {
            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.String);
            schema.Add("PK2", ColumnValueType.Integer);
            schema.Add("PK3", ColumnValueType.Integer);
            
            CreateTestTable("Table1", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table2", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table3", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table4", schema, new CapacityUnit(0, 0), false);
            WaitForTableReady();
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            request.Add("Table1", rowChange);
            request.Add("Table2", rowChange);
            request.Add("Table3", rowChange);
            request.Add("Table4", rowChange);
            
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            var item1 = GetNewBatchWriteRowResponseForOneTable();
            var item2 = GetNewBatchWriteRowResponseForOneTable();
            var item3 = GetNewBatchWriteRowResponseForOneTable();
            var item4 = GetNewBatchWriteRowResponseForOneTable();

            for (int i = 0; i < 4; i ++) {
                item1.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table1", i));
            }

            for (int i = 0; i < 4; i++)
            {
                item2.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table2", i));
            }

            for (int i = 0; i < 4; i++)
            {
                item3.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table3", i));
            }

            for (int i = 0; i < 4; i++)
            {
                item4.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table4", i));
            }
            expectResponse.TableRespones.Add("Table1", item1);
            expectResponse.TableRespones.Add("Table2", item2);
            expectResponse.TableRespones.Add("Table3", item3);
            expectResponse.TableRespones.Add("Table4", item4);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow("Table1", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table2", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table3", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table4", PrimaryKeyList[i], AttributeColumnsList[i]);
            }
        }

        /// <summary>
        /// BatchWriteRow包含1000个表的情况，期望返回服务端错误？
        /// </summary>
        [Test]
        public void Test1000TablesInBatchWriteRow() 
        {

            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.String);
            schema.Add("PK2", ColumnValueType.Integer);
            schema.Add("PK3", ColumnValueType.Integer);
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.IGNORE), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            for (int i = 0; i < 1000; i ++) {
                request.Add("Table" + i, rowChange);
            }
            
            try {
                var response = OTSClient.BatchWriteRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchWriteRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "Rows count exceeds the upper limit"
                ), exception);
            }         
        }

        /// <summary>
        /// BatchWriteRow有一个表中的一行失败的情况
        /// </summary>
        [Test]
        public void TestOneTableOneFailInBatchWriteRow() 
        {
            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.String);
            schema.Add("PK2", ColumnValueType.Integer);
            schema.Add("PK3", ColumnValueType.Integer);
            
            CreateTestTable("Table1", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table2", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table3", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table4", schema, new CapacityUnit(0, 0), false);
            WaitForTableReady();
            
            PutSingleRow("Table1", PrimaryKeyList[0], AttributeColumnsList[0]);
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            request.Add("Table1", rowChange);
            request.Add("Table2", rowChange);
            request.Add("Table3", rowChange);
            request.Add("Table4", rowChange);
            
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            
            for (int t = 1; t < 5; t ++) {
                var item = GetNewBatchWriteRowResponseForOneTable();
                for (int i = 0; i < 4; i ++) {
                    item.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table" + t, i));
                }
                expectResponse.TableRespones.Add("Table" + t, item);
            }
            
            expectResponse.TableRespones["Table1"].PutResponses[0] =
                new BatchWriteRowResponseItem("OTSConditionCheckFail", "Condition check failed.", "Table1", 0);
            
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow("Table1", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table2", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table3", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table4", PrimaryKeyList[i], AttributeColumnsList[i]);
            }
        }

        /// <summary>
        /// BatchWriteRow有一个表中的2行失败的情况
        /// </summary>
        [Test]
        public void TestOneTableTwoFailInBatchWriteRow() 
        {
            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.String);
            schema.Add("PK2", ColumnValueType.Integer);
            schema.Add("PK3", ColumnValueType.Integer);
            
            CreateTestTable("Table1", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table2", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table3", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table4", schema, new CapacityUnit(0, 0), false);
            WaitForTableReady();
            
            PutSingleRow("Table1", PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow("Table1", PrimaryKeyList[3], AttributeColumnsList[3]);
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            request.Add("Table1", rowChange);
            request.Add("Table2", rowChange);
            request.Add("Table3", rowChange);
            request.Add("Table4", rowChange);
            
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            
            for (int t = 1; t < 5; t ++) {
                var item = GetNewBatchWriteRowResponseForOneTable();
                for (int i = 0; i < 4; i ++) {
                    item.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table" + t, i));
                }
                expectResponse.TableRespones.Add("Table" + t, item);
            }
            
            expectResponse.TableRespones["Table1"].PutResponses[0] =
                new BatchWriteRowResponseItem("OTSConditionCheckFail", "Condition check failed.", "Table1", 0);
            expectResponse.TableRespones["Table1"].PutResponses[3] =
                new BatchWriteRowResponseItem("OTSConditionCheckFail", "Condition check failed.", "Table1", 3);
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow("Table1", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table2", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table3", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table4", PrimaryKeyList[i], AttributeColumnsList[i]);
            }
        }

        /// <summary>
        /// BatchWriteRow有2个表各有1行失败的情况
        /// </summary>
        [Test]
        public void TestTwoTableOneFailInBatchWriteRow() {
            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.String);
            schema.Add("PK2", ColumnValueType.Integer);
            schema.Add("PK3", ColumnValueType.Integer);
            
            CreateTestTable("Table1", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table2", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table3", schema, new CapacityUnit(0, 0), false);
            CreateTestTable("Table4", schema, new CapacityUnit(0, 0), false);
            WaitForTableReady();
            
            PutSingleRow("Table1", PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow("Table2", PrimaryKeyList[3], AttributeColumnsList[3]);
            
            var request = new BatchWriteRowRequest();
            var rowChange = new RowChanges();
            for (int i = 0; i < 4; i ++) {
                rowChange.AddPut(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST), PrimaryKeyList[i], AttributeColumnsList[i]);
            }
            
            request.Add("Table1", rowChange);
            request.Add("Table2", rowChange);
            request.Add("Table3", rowChange);
            request.Add("Table4", rowChange);
            
            var response = OTSClient.BatchWriteRow(request);
            
            var expectResponse = new BatchWriteRowResponse();
            
            for (int t = 1; t < 5; t ++) {
                var item = GetNewBatchWriteRowResponseForOneTable();
                for (int i = 0; i < 4; i ++) {
                    item.PutResponses.Add(new BatchWriteRowResponseItem(new CapacityUnit(0, 1), "Table" + t, i));
                }
                expectResponse.TableRespones.Add("Table" + t, item);
            }
            
            expectResponse.TableRespones["Table1"].PutResponses[0] =
                new BatchWriteRowResponseItem("OTSConditionCheckFail", "Condition check failed.", "Table1", 0);
            expectResponse.TableRespones["Table2"].PutResponses[3] =
                new BatchWriteRowResponseItem("OTSConditionCheckFail", "Condition check failed.", "Table1", 3);
            AssertBatchWriteRowResponse(expectResponse, response);
            
            for (int i = 0; i < 4; i ++) {
                CheckSingleRow("Table1", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table2", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table3", PrimaryKeyList[i], AttributeColumnsList[i]);
                CheckSingleRow("Table4", PrimaryKeyList[i], AttributeColumnsList[i]);
            }
        }
        #endregion

        #region BatchGetRowTest

        /// <summary>
        /// BatchGetRow没有包含任何表的情况。
        /// </summary>
        [Test]
        public void TestEmptyBatchGetRow() {
            var request = new BatchGetRowRequest();
            
            try {
                OTSClient.BatchGetRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchGetRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "No row specified in the request of BatchGetRow."
                ), exception);
            }
        }

        /// <summary>
        /// BatchGetRow包含2个表，其中有1个表有1行，另外一个表为空的情况。
        /// </summary>
        [Test]
        public void TestEmptyTableInBatchGetRow() 
        {
            var request = new BatchGetRowRequest();
            request.Add(TestTableName, PrimaryKeyList.GetRange(0, 4));
            request.Add("Table2", new List<PrimaryKey>());
            
            try {
                OTSClient.BatchGetRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchGetRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "No row specified in table: 'Table2'."
                ), exception);
            }
        }

        /// <summary>
        /// BatchGetRow包含4个行。
        /// </summary>
        [Test]
        public void Test4ItemInBatchGetRow() 
        {
            CreateTestTableWith4PK();
            PutSingleRow(TestTableName, PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow(TestTableName, PrimaryKeyList[1], AttributeColumnsList[1]);
            
            var request = new BatchGetRowRequest();
            request.Add(TestTableName, PrimaryKeyList.GetRange(0, 4));
            
            var response = OTSClient.BatchGetRow(request);
            
            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();
            
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[0], AttributeColumnsList[0]));
            
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[1], AttributeColumnsList[1]));
                        
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));
            
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));
            
            responseToExpect.Add(TestTableName, rowDataInTable);
            
            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow包含1000个行，期望返回服务端错误？
        /// </summary>
        [Test]
        public void Test1000ItemInBatchGetRow() 
        { 
            var request = new BatchGetRowRequest();
            request.Add(TestTableName, PrimaryKeyList.GetRange(0, 1000));
            
            try {
                OTSClient.BatchGetRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/BatchGetRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "Rows count exceeds the upper limit"
                ), exception);
            }
        }

        /// <summary>
        /// BatchGetRow有一个表中的一行失败的情况
        /// </summary>
        [Test]
        public void TestOneTableOneFailInBatchGetRow() { }

        /// <summary>
        /// BatchGetRow有一个表中的2行失败的情况
        /// </summary>
        [Test]
        public void TestOneTableTwoFailInBatchGetRow() { }

        /// <summary>
        /// BatchGetRow有2个表各有1行失败的情况
        /// </summary>
        [Test]
        public void TestTwoTableOneFailInBatchGetRow() { }

        /// <summary>
        /// BatchGetRow只取一行，这一行不会被过滤掉
        /// </summary>
        [Test]
        public void TestOneItemInBatchGetRowWithoutFilter() 
        {
            CreateTestTableWith4PK();
            PutSingleRow(TestTableName, PrimaryKeyList[0], AttributeColumnsList[0]);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(123));

            var request = new BatchGetRowRequest();
            request.Add(TestTableName, PrimaryKeyList.GetRange(0, 1), null, filter);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();

            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[0], AttributeColumnsList[0]));

            responseToExpect.Add(TestTableName, rowDataInTable);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow只取一行，这一行会被过滤掉
        /// </summary>
        [Test]
        public void TestOneItemInBatchGetRowWithFilter()
        {
            CreateTestTableWith4PK();
            PutSingleRow(TestTableName, PrimaryKeyList[0], AttributeColumnsList[0]);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(123));

            var request = new BatchGetRowRequest();
            request.Add(TestTableName, PrimaryKeyList.GetRange(0, 1), null, filter);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();

            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));

            responseToExpect.Add(TestTableName, rowDataInTable);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow取两行，这两行都不会被过滤掉
        /// </summary>
        [Test]
        public void TestTwoItemInBatchGetRowWithoutFilter() 
        {
            var tableName = TestTableName;
            var tableName2 = TestTableName + "_2";
            CreateTestTableWith4PK(null, tableName);
            CreateTestTableWith4PK(null, tableName2);

            PutSingleRow(tableName, PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow(tableName2, PrimaryKeyList[0], AttributeColumnsList[0]);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(123));

            var request = new BatchGetRowRequest();
            request.Add(tableName, PrimaryKeyList.GetRange(0, 1), null, filter);
            request.Add(tableName2, PrimaryKeyList.GetRange(0, 1), null, filter);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();

            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[0], AttributeColumnsList[0]));

            responseToExpect.Add(tableName, rowDataInTable);
            responseToExpect.Add(tableName2, rowDataInTable);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow取两行，其中一行会被过滤掉
        /// </summary>
        [Test]
        public void TestTwoItemInBatchGetRowWithOneFilter()
        {
            var tableName = TestTableName;
            var tableName2 = TestTableName + "_2";
            CreateTestTableWith4PK(null, tableName);
            CreateTestTableWith4PK(null, tableName2);

            PutSingleRow(tableName, PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow(tableName2, PrimaryKeyList[0], AttributeColumnsList[0]);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(123));
            var filter2 = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(123));

            var request = new BatchGetRowRequest();
            request.Add(tableName, PrimaryKeyList.GetRange(0, 1), null, filter);
            request.Add(tableName2, PrimaryKeyList.GetRange(0, 1), null, filter2);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[0], AttributeColumnsList[0]));

            var rowDataInTable2 = new List<BatchGetRowResponseItem>();
            rowDataInTable2.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));

            responseToExpect.Add(tableName, rowDataInTable);
            responseToExpect.Add(tableName2, rowDataInTable2);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow取两行，其中两行都被过滤掉
        /// </summary>
        [Test]
        public void TestTwoItemInBatchGetRowWithTwoFilter()
        {
            var tableName = TestTableName;
            var tableName2 = TestTableName + "_2";
            CreateTestTableWith4PK(null, tableName);
            CreateTestTableWith4PK(null, tableName2);

            PutSingleRow(tableName, PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow(tableName2, PrimaryKeyList[1], AttributeColumnsList[1]);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.GREATER_THAN, new ColumnValue(123));
            var filter2 = new RelationalCondition("Col3", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(true));

            var request = new BatchGetRowRequest();
            request.Add(tableName, PrimaryKeyList.GetRange(0, 1), null, filter);
            request.Add(tableName2, PrimaryKeyList.GetRange(0, 1), null, filter2);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));

            var rowDataInTable2 = new List<BatchGetRowResponseItem>();
            rowDataInTable2.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));

            responseToExpect.Add(tableName, rowDataInTable);
            responseToExpect.Add(tableName2, rowDataInTable2);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        /// <summary>
        /// BatchGetRow取两行，其中两行的过滤条件中的列都不存在列中，
        /// 其中一个passIfMiss为true，一个为false
        /// </summary>
        [Test]
        public void TestTwoItemInBatchGetRowWithPassIfMiss()
        {
            var tableName = TestTableName;
            var tableName2 = TestTableName + "_2";
            CreateTestTableWith4PK(null, tableName);
            CreateTestTableWith4PK(null, tableName2);

            PutSingleRow(tableName, PrimaryKeyList[0], AttributeColumnsList[0]);
            PutSingleRow(tableName2, PrimaryKeyList[1], AttributeColumnsList[1]);

            var filter = new RelationalCondition("Col5", RelationalCondition.CompareOperator.GREATER_THAN, new ColumnValue(123))
            {
                PassIfMissing = true
            };
            var filter2 = new RelationalCondition("Col5", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(true))
            {
                PassIfMissing = false
            };

            var request = new BatchGetRowRequest();

            var criteria1 = new MultiRowQueryCriteria(tableName)
            {
                Filter = filter
            };
            criteria1.SetRowKeys(PrimaryKeyList.GetRange(0, 1));

            var criteria2 = new MultiRowQueryCriteria(tableName2)
            {
                Filter = filter2
            };
            criteria2.SetRowKeys(PrimaryKeyList.GetRange(0, 1));

            request.Add(criteria1);
            request.Add(criteria2);

            var response = OTSClient.BatchGetRow(request);

            var responseToExpect = new BatchGetRowResponse();
            var rowDataInTable = new List<BatchGetRowResponseItem>();
            rowDataInTable.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), PrimaryKeyList[0], AttributeColumnsList[0]));

            var rowDataInTable2 = new List<BatchGetRowResponseItem>();
            rowDataInTable2.Add(new BatchGetRowResponseItem(
                new CapacityUnit(1, 0), new PrimaryKey(), new AttributeColumns()));

            responseToExpect.Add(tableName, rowDataInTable);
            responseToExpect.Add(tableName2, rowDataInTable2);

            AssertBatchGetRowResponse(responseToExpect, response);
        }

        #endregion

        #region GetRangeTest

        /// <summary>
        /// 先写入两行，PK分别为1和2，GetRange，方向为Forward，期望顺序得到1和2两行。
        /// </summary>
        [Test]
        public void TestGetRangeForward() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              PrimaryKeyList[0], PrimaryKeyList[3]);
            
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);
            
            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[0].Attribute);
            
            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[1].Attribute);
        }

        /// <summary>
        /// 先写入两行，PK分别为1和2，GetRange，方向为Backward，期望顺序得到2和1两行。
        /// </summary>
        [Test]
        public void TestGetRangeBackward() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Backward, 
                                              PrimaryKeyList[3], PrimaryKeyList[0]);
            
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);
            
            AssertPrimaryKey(PrimaryKeyList[2], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[2], response.RowDataList[0].Attribute);
            
            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[1].Attribute);
            
        }
        
        /// <summary>
        /// 先写入2行，PK分别为0, 1和2，GetRange，方向为Forward，范围为 [INF_MIN, 2) 或者 [1, INF_MIN)，期望顺序得到0和1两行，或者返回服务端错误
        /// </summary>
        [Test]
        public void TestInfMinInRange() 
        { 
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, PrimaryKeyList[2]);
            
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);
            
            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[0].Attribute);
            
            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[1].Attribute); 
            
            
            try {
            
                request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              PrimaryKeyList[1], MinPrimaryKeyWith4Columns);
                response = OTSClient.GetRange(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/GetRange", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "The input parameter is invalid."
                ), exception);
            }
        }

        /// <summary>
        /// 先写入2行，PK分别为0, 1和2，GetRange，方向为Forward，范围为 [INF_MAX, 2) 或者 [1, INF_MAX)，期望返回服务端错误，或者顺序得到1和2两行
        /// </summary>
        [Test]
        public void TestInfMaxInRange() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              PrimaryKeyList[1], MaxPrimaryKeyWith4Columns);
            
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);
            
            AssertGetRangeRowWithPredefinedRow(response.RowDataList[0], 1);
            AssertGetRangeRowWithPredefinedRow(response.RowDataList[1], 2);
            
            try {
            
                request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MaxPrimaryKeyWith4Columns, PrimaryKeyList[1]);
                response = OTSClient.GetRange(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/GetRange", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "The input parameter is invalid."
                ), exception);
            }
        }

        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRange请求ColumnsToGet参数为默认，期望读出所有4个属性列。
        /// </summary>
        [Test]
        public void TestGetRangeWithDefaultColumnsToGet() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns);
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(1, response.RowDataList.Count);
            
            AssertGetRangeRowWithPredefinedRow(response.RowDataList[0], 0);
        }

        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRange请求ColumnsToGet为空数组，期望读出所有4个属性列。
        /// </summary>
        [Test]
        public void TestGetRangeWithColumsToGetIsEmpty() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                              new HashSet<string>());
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(1, response.RowDataList.Count);
            
            AssertGetRangeRowWithPredefinedRow(response.RowDataList[0], 0);
        }

        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRange请求ColumnsToGet包含其中2个主键列，2个属性列，期望返回数据包含参数中指定的列。
        /// </summary>
        [Test]
        public void TestGetRangeWith4ColumnsToGet() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            
            var columnsToGet = new HashSet<string>() {"PK0", "PK1", "Col0", "Col1"};
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                              columnsToGet);
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(1, response.RowDataList.Count);
            
            var primaryKeyToExpect = new PrimaryKey();
            primaryKeyToExpect.Add("PK0", new ColumnValue("ABC"));
            primaryKeyToExpect.Add("PK1", new ColumnValue("DEF"));
            
            var attributeToExpect = new AttributeColumns();
            attributeToExpect.Add("Col0", new ColumnValue("ABC"));
            attributeToExpect.Add("Col1", new ColumnValue(123));
            
            AssertPrimaryKey(primaryKeyToExpect, response.RowDataList[0].PrimaryKey);
            AssertAttribute(attributeToExpect, response.RowDataList[0].Attribute);
        }

        /// <summary>
        /// GetRange请求ColumnsToGet包含1000个不重复的列名，期望返回服务端错误？
        /// </summary>
        [Test]
        public void TestGetRangeWith1025ColumnsToGet() {
            
            var columnsToGet = new HashSet<string>();
            
            for (int i = 0; i < 1025; i ++)
            {
                columnsToGet.Add("Col" + i);
            }
            
            try {
            
                var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MaxPrimaryKeyWith4Columns, PrimaryKeyList[1],
                                             columnsToGet);
                var response = OTSClient.GetRange(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/GetRange", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "The number of columns from the request exceeded the limit."
                ), exception);
            }
        }

        /// <summary>
        /// C# SDK 特殊
        /// GetRange请求ColumnsToGet包含2个重复的列名，期望正常返回。
        /// </summary>
        [Test]
        public void TestGetRangeWithDuplicateColumnsToGet() 
        {
            
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            
            var columnsToGet = new HashSet<string>() {"PK0", "PK1", "Col0", "Col1"};
            columnsToGet.Add("PK0");
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                              columnsToGet);
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(1, response.RowDataList.Count);
            
            var primaryKeyToExpect = new PrimaryKey();
            primaryKeyToExpect.Add("PK0", new ColumnValue("ABC"));
            primaryKeyToExpect.Add("PK1", new ColumnValue("DEF"));
            
            var attributeToExpect = new AttributeColumns();
            attributeToExpect.Add("Col0", new ColumnValue("ABC"));
            attributeToExpect.Add("Col1", new ColumnValue(123));
            
            AssertPrimaryKey(primaryKeyToExpect, response.RowDataList[0].PrimaryKey);
            AssertAttribute(attributeToExpect, response.RowDataList[0].Attribute);
        }

        /// <summary>
        /// 先写入20行，GetRange Limit为默认，期望返回20行；GetRange Limit=10，期望返回10行，并校验 NextPK。
        /// </summary>
        [Test]
        public void TestGetRangeWithLimit10() 
        {
            CreateTestTableWith4PK();
            for (int i = 0; i < 20; i ++) {
                PutSinglePredefinedRow(i);
            }
            
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                              MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns);
            var response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(20, response.RowDataList.Count);
            
            for (int i = 0; i < 20; i ++) {
                AssertGetRangeRowWithPredefinedRow(response.RowDataList[i], i);
            }
            
            request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward, 
                                          MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                          limit : 10);
            response = OTSClient.GetRange(request);
            
            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(PrimaryKeyList[10], response.NextPrimaryKey);
            Assert.AreEqual(10, response.RowDataList.Count);
            
            for (int i = 0; i < 10; i ++) {
                AssertGetRangeRowWithPredefinedRow(response.RowDataList[i], i);
            }
        }

        /// <summary>
        /// 先写入两行，PK分别为1，2，3，GetRange，方向为Forward，过滤掉2，期望顺序得到1和3两行。
        /// </summary>
        [Test]
        public void TestGetRangeForwardWithFilterMidlleRolw() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(124));
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward,
                                              PrimaryKeyList[0], PrimaryKeyList[3], condition : filter);

            var response = OTSClient.GetRange(request);

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);

            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[0].Attribute);

            AssertPrimaryKey(PrimaryKeyList[2], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[2], response.RowDataList[1].Attribute);
        }

        /// <summary>
        /// 先写入两行，PK分别为1，2，3，GetRange，方向为Forward，过滤掉1，期望顺序得到2和3两行。
        /// </summary>
        [Test]
        public void TestGetRangeForwardWithFilterFirstRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(123));
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward,
                                              PrimaryKeyList[0], PrimaryKeyList[3], condition: filter);

            var response = OTSClient.GetRange(request);

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);

            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[0].Attribute);

            AssertPrimaryKey(PrimaryKeyList[2], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[2], response.RowDataList[1].Attribute);
        }

        /// <summary>
        /// 先写入两行，PK分别为1，2，3，GetRange，方向为Forward，过滤掉3，期望顺序得到1和2两行。
        /// </summary>
        [Test]
        public void TestGetRangeForwardWithFilterLastRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(125));
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward,
                                              PrimaryKeyList[0], PrimaryKeyList[3], condition: filter);

            var response = OTSClient.GetRange(request);

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);

            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[0].Attribute);

            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[1].Attribute);
        }

        /// <summary>
        /// 先写入两行，PK分别为1，2，3，GetRange，方向为Backward，过滤掉3，期望顺序得到2和1两行。
        /// </summary>
        [Test]
        public void TestGetRangeBackwardWithFilterLastRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);
            PutSinglePredefinedRow(3);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(126));
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Backward,
                                              PrimaryKeyList[3], PrimaryKeyList[0], condition: filter);

            var response = OTSClient.GetRange(request);

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(2, response.RowDataList.Count);

            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[0].Attribute);

            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[1].Attribute);
        }

        /// <summary>
        /// 先写入两行，PK分别为1，2，3，GetRange，方向为Forward，过滤掉1，2，3，返回空
        /// </summary>
        [Test]
        public void TestGetRangeForwardWithFilterAllRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);

            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.LESS_EQUAL, new ColumnValue(1));
            var criteria = new RangeRowQueryCriteria(TestTableName)
            {
                Direction = GetRangeDirection.Forward,
                InclusiveStartPrimaryKey = PrimaryKeyList[0],
                ExclusiveEndPrimaryKey = PrimaryKeyList[3],
                Filter = filter
            };

            var response = OTSClient.GetRange(new GetRangeRequest(criteria));

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(0, response.RowDataList.Count);
        }

        /// <summary>
        /// 先写入三行，PK分别为1，2，3，GetRange，方向为Forward，按不存在的列Col5过滤，期望顺序得到1，2，3三行。
        /// </summary>
        [Test]
        public void TestGetRangeForwardWithFilterNotExistCols()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);

            var filter = new RelationalCondition("Col5", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(125));
            var request = new GetRangeRequest(TestTableName, GetRangeDirection.Forward,
                                              PrimaryKeyList[0], PrimaryKeyList[3], condition: filter);

            var response = OTSClient.GetRange(request);

            AssertCapacityUnit(new CapacityUnit(1, 0), response.ConsumedCapacityUnit);
            AssertPrimaryKey(null, response.NextPrimaryKey);
            Assert.AreEqual(3, response.RowDataList.Count);

            AssertPrimaryKey(PrimaryKeyList[0], response.RowDataList[0].PrimaryKey);
            AssertAttribute(AttributeColumnsList[0], response.RowDataList[0].Attribute);

            AssertPrimaryKey(PrimaryKeyList[1], response.RowDataList[1].PrimaryKey);
            AssertAttribute(AttributeColumnsList[1], response.RowDataList[1].Attribute);

            AssertPrimaryKey(PrimaryKeyList[2], response.RowDataList[2].PrimaryKey);
            AssertAttribute(AttributeColumnsList[2], response.RowDataList[2].Attribute);
        }

        #endregion

        #region GetRangeIteratorTest

        /// <summary>
        /// GetRangeIterator 返回0行的情况。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWith0Row() 
        {
            CreateTestTableWith4PK();
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                        MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                        consumed);
            var iterator = OTSClient.GetRangeIterator(request);
            int i = 0;
            foreach (var rowData in iterator)
            {
                i ++;
            }
            
            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(0, i);
        }

        /// <summary>
        /// GetRangeIterator 返回1行的情况。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWith1Row() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
            MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
            consumed);
            var iterator = OTSClient.GetRangeIterator(request);

            int i = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, i);
                i += 1;
            }
            
            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(1, i);
        }

        /// <summary>
        /// GetRangeIterator 返回5000行的情况，这时正好不发生第二次GetRange。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWith5000Rows() 
        {
            CreateTestTableWith4PK(new CapacityUnit(5000, 5000));
            PutPredefinedRows(5000);
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                            MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns, consumed);
            var iterator = OTSClient.GetRangeIterator(request);

            int j = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, j);
                j += 1;
            }
            
            AssertCapacityUnit(new CapacityUnit(107, 0), consumed);
            Assert.AreEqual(5000, j);
        }

        /// <summary>
        /// GetRangeIterator 返回5001行的情况，这时正好发生第二次GetRange。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWith5001Rows() 
        {
            CreateTestTableWith4PK(new CapacityUnit(5000, 5000));
            PutPredefinedRows(5001);
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
            MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns, consumed);
            var iterator = OTSClient.GetRangeIterator(request);

            int j = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, j);
                j += 1;
            }
            
            AssertCapacityUnit(new CapacityUnit(108, 0), consumed);
            Assert.AreEqual(5001, j);
        }

        /// <summary>
        /// GetRangeIterator 返回15001行的情况，这时共发生4次GetRange。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWith15001Rows() 
        {
            CreateTestTableWith4PK(new CapacityUnit(5000, 5000));
            PutPredefinedRows(15100);
            
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                    MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                    consumed, null, 15001);
            var iterator = OTSClient.GetRangeIterator(request);

            int j = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, j);
                j += 1;
            }

            Assert.AreEqual(15001, j);
            AssertCapacityUnit(new CapacityUnit(328, 0), consumed);
        }
        
        /// <summary>
        /// 先写入20行，GetRangeIterator Count=10，期望返回10行； GetRangeIterator count为默认值，期望返回20行。
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWithCount() 
        {
            CreateTestTableWith4PK();
            PutPredefinedRows(20);
            
            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                    MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                    consumed, null, 10);
            var iterator = OTSClient.GetRangeIterator(request);

            int i = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, i);
                i += 1;
            }
            
            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(10, i);
            consumed = new CapacityUnit(0, 0);
            request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                        MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                        consumed);
            iterator = OTSClient.GetRangeIterator(request);

            i = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, i);
                i += 1;
            }
            
            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(20, i);
        }

        /// <summary>
        /// 先写入3行，过滤掉第一行，返回第二行和第三行
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWithFilterFirstRow() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);


            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(123));

            var consumed = new CapacityUnit(0, 0);

            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                                MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                                consumed, condition: filter);
            var iterator = OTSClient.GetRangeIterator(request);
            int i = 1;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, i);
                i += 1;
            }

            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(3, i);
        }

        /// <summary>
        /// 先写入3行，过滤掉第二行，返回第一行和第三行
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWithFilterMiddleRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);


            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(124));

            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                    MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                    consumed, condition: filter);
            var iterator = OTSClient.GetRangeIterator(request);

            int i = 0;
            foreach (var rowData in iterator)
            {
                if (i != 1)
                {
                    AssertGetRangeRowWithPredefinedRow(rowData, i);
                }
                i += 1;
            }

            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(2, i);
        }

        /// <summary>
        /// 先写入3行，过滤掉第三行，返回第一行和第二行
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWithFilterLastRow()
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);


            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.NOT_EQUAL, new ColumnValue(125));

            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                                MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                                consumed, condition: filter);
            var iterator = OTSClient.GetRangeIterator(request);
            int i = 0;
            foreach (var rowData in iterator)
            {
                if (i != 2)
                {
                    AssertGetRangeRowWithPredefinedRow(rowData, i);
                }
                i += 1;
            }

            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(2, i);
        }

        /// <summary>
        /// 先写入3行，过滤掉全部行，返回空
        /// </summary>
        [Test]
        public void TestGetRangeIteratorWithFilterAllRow() 
        {
            CreateTestTableWith4PK();
            PutSinglePredefinedRow(0);
            PutSinglePredefinedRow(1);
            PutSinglePredefinedRow(2);


            var filter = new RelationalCondition("Col1", RelationalCondition.CompareOperator.EQUAL, new ColumnValue(1));

            var consumed = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TestTableName, GetRangeDirection.Forward,
                                                MinPrimaryKeyWith4Columns, MaxPrimaryKeyWith4Columns,
                                                consumed, condition: filter);
            var iterator = OTSClient.GetRangeIterator(request);
            int i = 0;
            foreach (var rowData in iterator)
            {
                AssertGetRangeRowWithPredefinedRow(rowData, i);
            }

            AssertCapacityUnit(new CapacityUnit(1, 0), consumed);
            Assert.AreEqual(0, i);
        }

        #endregion
    }
}
