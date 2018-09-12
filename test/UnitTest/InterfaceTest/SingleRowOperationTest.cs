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
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.UnitTest.InterfaceTest
{

    [TestFixture]
    class SingleRowOperationTest : OTSUnitTestBase
    {
        #region GetRowTest
        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRow请求ColumnsToGet参数为默认，期望读出所有4个主键列和4个属性列。
        /// </summary>
        [Test]
        public void TestGetRowWithDefaultColumnsToGet() 
        {
            CreateTestTableWith4PK();

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(1) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) },
                { "Col3", new ColumnValue(4) }
            };

            PutSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute);
            CheckSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute, new CapacityUnit(1, 0));
        }

        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRow请求ColumnsToGet为空数组，期望读出所有4个主键列和4个属性列。
        /// </summary>
        [Test]
        public void TestGetRowWith0ColumsToGet() 
        {
            CreateTestTableWith4PK();

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(1) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) },
                { "Col3", new ColumnValue(4) }
            };

            PutSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute);
            
            var columnsToGet = new HashSet<string>();
            CheckSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute, new CapacityUnit(1, 0), columnsToGet);
        }

        /// <summary>
        /// 先PutRow包含4个主键列，4个属性列，然后GetRow请求ColumnsToGet包含其中2个主键列，2个属性列，期望返回数据包含参数中指定的列。
        /// </summary>
        [Test]
        public void TestGetRowWith4ColumnsToGet() 
        {
            CreateTestTableWith4PK();

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(1) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) },
                { "Col3", new ColumnValue(4) }
            };

            PutSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute);
            
            var columnsToGet = new HashSet<string> {"PK0", "PK1", "Col0", "Col1"};
            
            CheckSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute, new CapacityUnit(1, 0), columnsToGet);
            DeleteTable();
        }

        /// <summary>
        /// GetRow请求ColumnsToGet包含1000个不重复的列名，期望返回服务端错误？
        /// </summary>
        [Test]
        public void TestGetRowWith1000ColumnsToGet() 
        {
            var columnsToGet = new HashSet<string>();
            for (int i = 0; i < 1025; i ++) {
                columnsToGet.Add("Col" + i);
            }
            
            var request = new GetRowRequest(TestTableName, PrimaryKeyWith4Columns, columnsToGet);
            
            try {
                OTSClient.GetRow(request);
                Assert.Fail();
            } catch (OTSServerException exception) {
                AssertOTSServerException(new OTSServerException(
                    "/GetRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "The number of columns from the request exceeds the limit, limit count: 1024, column count: 1025."
                ), exception);
            }
        }

        /// <summary>
        /// C# SDK 特殊:GetRow请求ColumnsToGet包含2个重复的列名，期望SDK对列名去重，并正常读取数据。
        /// </summary>
        [Test]
        public void TestGetRowWithDuplicateColumnsToGet() 
        {
            CreateTestTableWith4PK();

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(1) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) },
                { "Col3", new ColumnValue(4) }
            };

            PutSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute);
            
            var columnsToGet = new HashSet<string>() {"PK0", "PK1", "Col0", "Col1"};
            columnsToGet.Add("PK0");
            
            CheckSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute, new CapacityUnit(1, 0), columnsToGet);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5，都满足，全部Row都返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterZeroRow() 
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, condition : filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5，都满足，全部Row都返回，只需要col1
        /// </summary>
        [Test]
        public void TestGetRowWithFilterZeroRowAndReturnOneColumn()
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>
            {
                "Col1"
            };
            var filter = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0小于5，都不满足，返回空
        /// </summary>
        [Test]
        public void TestGetRowWithFilterOneRow()
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter = new RelationalCondition("Col0", CompareOperator.LESS_THAN, new ColumnValue(5));

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, isEmpty: true, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0小于5，只返回Col1，Row都不满足，返回空
        /// </summary>
        [Test]
        public void TestGetRowWithFilterOneRowAndReturnCol1()
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("aff") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>
            {
                "Col0",
                "Col1"
            };

            var filter = new RelationalCondition("Col0", CompareOperator.LESS_THAN, new ColumnValue(5));

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, isEmpty:true, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5且Col1等于2，组合条件都满足，全部Row都返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterAndLogicAndAllHit() 
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter1 = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));
            var filter2 = new RelationalCondition("Col1", CompareOperator.EQUAL, new ColumnValue(2));
            var filter = new CompositeCondition(LogicOperator.AND);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5且Col1不等于2，组合条件不满足，无返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterAndLogicAndPartHit()
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter1 = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));
            var filter2 = new RelationalCondition("Col1", CompareOperator.NOT_EQUAL, new ColumnValue(2));
            var filter = new CompositeCondition(LogicOperator.AND);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, true, filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5或Col1不等于2，组合条件满足，Row返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterAndLogicOrPartHit() 
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter1 = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));
            var filter2 = new RelationalCondition("Col1", CompareOperator.NOT_EQUAL, new ColumnValue(2));
            var filter = new CompositeCondition(Aliyun.OTS.DataModel.LogicOperator.OR);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0大于5或Col1等于10，组合条件都满足，Row返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterAndLogicOrAllHit()
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter1 = new RelationalCondition("Col0", CompareOperator.GREATER_THAN, new ColumnValue(5));
            var filter2 = new RelationalCondition("Col1", CompareOperator.EQUAL, new ColumnValue(2));
            var filter = new CompositeCondition(Aliyun.OTS.DataModel.LogicOperator.OR);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, condition: filter);
        }

        /// <summary>
        /// 按过滤条件查询数据，过滤条件：Col0小于5或Col1不等于2，组合条件都不满足，无返回
        /// </summary>
        [Test]
        public void TestGetRowWithFilterAndLogicOrAllNotHit() 
        {
            CreateTestTableWith4PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("a") },
                { "PK1", new ColumnValue("a") },
                { "PK2", new ColumnValue(10) },
                { "PK3", new ColumnValue(20) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(10) },
                { "Col1", new ColumnValue(2) },
                { "Col2", new ColumnValue(3) }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var columnsToGet = new HashSet<string>();
            var filter1 = new RelationalCondition("Col0", CompareOperator.LESS_EQUAL, new ColumnValue(5));
            var filter2 = new RelationalCondition("Col1", CompareOperator.NOT_EQUAL, new ColumnValue(2));
            var filter = new CompositeCondition(Aliyun.OTS.DataModel.LogicOperator.OR);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            CheckSingleRow(TestTableName, pk, attribute, new CapacityUnit(1, 0), columnsToGet, isEmpty:true, condition: filter);
        }

        #endregion

        #region UpdateRowTest
        // <summary>
        // UpdateRow包含4个属性列的put操作的情况。
        // </summary>
        [Test]
        public void TestPutOnlyInUpdateRow() 
        {
            CreateTestTableWith2PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("123") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col1", new ColumnValue("1") },
                { "Col2", new ColumnValue("2") },
                { "Col3", new ColumnValue("3") },
                { "Col4", new ColumnValue("4") }
            };

            PutSingleRow(TestTableName, pk, attribute);
            
            var updateOfAttribute = new UpdateOfAttribute();

            updateOfAttribute.AddAttributeColumnToPut("Col3", new ColumnValue("5"));
            updateOfAttribute.AddAttributeColumnToPut("Col4", new ColumnValue("6"));
            updateOfAttribute.AddAttributeColumnToPut("Col5", new ColumnValue("7"));
            updateOfAttribute.AddAttributeColumnToPut("Col6", new ColumnValue("8"));

            var request = new UpdateRowRequest(
                TestTableName, new Condition(RowExistenceExpectation.IGNORE), pk, updateOfAttribute);
            var response = OTSClient.UpdateRow(request);
            AssertCapacityUnit(new CapacityUnit(0, 1), response.ConsumedCapacityUnit);

            var expectAttribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col1", new ColumnValue("1") },
                { "Col2", new ColumnValue("2") },
                { "Col3", new ColumnValue("5") },
                { "Col4", new ColumnValue("6") },
                { "Col5", new ColumnValue("7") },
                { "Col6", new ColumnValue("8") }
            };

            CheckSingleRow(TestTableName, pk, expectAttribute);
        }

        // <summary>
        // UpdateRow包含4个属性列的delete操作的情况。
        // </summary>
        [Test]
        public void TestDeleteOnlyInUpdateRow()
        {
            CreateTestTableWith2PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("123") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col1", new ColumnValue("1") },
                { "Col2", new ColumnValue("2") },
                { "Col3", new ColumnValue("3") },
                { "Col4", new ColumnValue("4") }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var updateOfAttribute = new UpdateOfAttribute();

            updateOfAttribute.AddAttributeColumnToDelete("Col1");
            updateOfAttribute.AddAttributeColumnToDelete("Col2");
            updateOfAttribute.AddAttributeColumnToDelete("Col7");
            updateOfAttribute.AddAttributeColumnToDelete("Col8");

            var request = new UpdateRowRequest(
                TestTableName, new Condition(RowExistenceExpectation.IGNORE), pk, updateOfAttribute);
            var response = OTSClient.UpdateRow(request);
            AssertCapacityUnit(new CapacityUnit(0, 1), response.ConsumedCapacityUnit);

            var expectAttribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col3", new ColumnValue("3") },
                { "Col4", new ColumnValue("4") }
            };

            CheckSingleRow(TestTableName, pk, expectAttribute);
        }

        // <summary>
        // UpdateRow没有包含任何操作的情况，期望返回服务端错误？
        // </summary>
        [Test]
        public void TestEmptyUpdateRow()
        {
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(0) }
            };
            var updateOfAttribute = new UpdateOfAttribute();

            var request = new UpdateRowRequest(TestTableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, updateOfAttribute);

            try
            {
                OTSClient.UpdateRow(request);
                Assert.Fail();
            }
            catch (OTSServerException e)
            {
                AssertOTSServerException(e, new OTSServerException(
                    "/UpdateRow",
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid",
                    "Attribute column is missing."
                ));
            }

        }

        // <summary>
        // UpdateRow中包含4个put操作和4个delete操作的情况。
        // </summary>
        [Test]
        public void Test4PutAnd4DeleteInUpdateRow()
        {
            CreateTestTableWith2PK();

            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("123") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col1", new ColumnValue("1") },
                { "Col2", new ColumnValue("2") },
                { "Col3", new ColumnValue("3") },
                { "Col4", new ColumnValue("4") }
            };

            PutSingleRow(TestTableName, pk, attribute);

            var updateOfAttribute = new UpdateOfAttribute();

            updateOfAttribute.AddAttributeColumnToPut("Col3", new ColumnValue("5"));
            updateOfAttribute.AddAttributeColumnToPut("Col4", new ColumnValue("6"));
            updateOfAttribute.AddAttributeColumnToPut("Col5", new ColumnValue("7"));
            updateOfAttribute.AddAttributeColumnToPut("Col6", new ColumnValue("8"));

            updateOfAttribute.AddAttributeColumnToDelete("Col1");
            updateOfAttribute.AddAttributeColumnToDelete("Col2");
            updateOfAttribute.AddAttributeColumnToDelete("Col7");
            updateOfAttribute.AddAttributeColumnToDelete("Col8");

            var request = new UpdateRowRequest(
                TestTableName, new Condition(RowExistenceExpectation.IGNORE), pk, updateOfAttribute);
            var response = OTSClient.UpdateRow(request);
            AssertCapacityUnit(new CapacityUnit(0, 1), response.ConsumedCapacityUnit);

            var expectAttribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("0") },
                { "Col3", new ColumnValue("5") },
                { "Col4", new ColumnValue("6") },
                { "Col5", new ColumnValue("7") },
                { "Col6", new ColumnValue("8") }
            };

            CheckSingleRow(TestTableName, pk, expectAttribute);
        }

        // <summary>
        // UpdateRow中包含2个delete操作列名相同的情况，不返回错误信息
        // </summary>
        [Test]
        public void TestDuplicateDeleteInUpdateRow() 
        {
            CreateTable();
            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("123") },
                { "PK1", new ColumnValue(123) }
            };

            var attributes = new AttributeColumns
            {
                { "Col0", new ColumnValue(123) }
            };

            PutSingleRow(TestTableName, pk, attributes);

            var updateOfAttribute = new UpdateOfAttribute();
            
            updateOfAttribute.AddAttributeColumnToDelete("Col0");
            updateOfAttribute.AddAttributeColumnToDelete("Col0");
            
            var request = new UpdateRowRequest(
                TestTableName, new Condition(RowExistenceExpectation.IGNORE), pk, updateOfAttribute);
            
            try {
                var response = OTSClient.UpdateRow(request);
                Assert.Pass();
            } catch (OTSServerException e) {
                AssertOTSServerException(e, new OTSServerException(
                    "/UpdateRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "Duplicated attribute column name: 'Col0' while updating row."
                ));
            }finally
            {
                DeleteTable();
            }
        }

        // <summary>
        // UpdateRow中包含1000个put和1000个delete的情况，期望返回服务端错误？
        // </summary>
        [Test]
        public void Test1000PutAnd1000DeleteInUpdateRow() 
        {
            var pk = new PrimaryKey
            {
                { "PK0", new ColumnValue("123") },
                { "PK1", new ColumnValue(123) }
            };


            var updateOfAttribute = new UpdateOfAttribute();
            
            for (int i = 0; i < 1000; i ++) {
                updateOfAttribute.AddAttributeColumnToPut("Put" + i, new ColumnValue("blah"));
                updateOfAttribute.AddAttributeColumnToDelete("Delete" + i);
            }
            
            var request = new UpdateRowRequest(
                TestTableName, new Condition(RowExistenceExpectation.IGNORE), pk, updateOfAttribute);
            
            try {
                var response = OTSClient.UpdateRow(request);
            } catch (OTSServerException e) {
                AssertOTSServerException(e, new OTSServerException(
                    "/UpdateRow", 
                    HttpStatusCode.BadRequest,
                    "OTSParameterInvalid", 
                    "The number of attribute columns exceeds the limit, limit count: 1024, column count: 2000."
                ));
            }
        }
        #endregion
    }
}
