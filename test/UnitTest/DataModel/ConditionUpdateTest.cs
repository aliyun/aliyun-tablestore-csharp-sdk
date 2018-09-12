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
using System.Threading;

using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.UnitTest.DataModel
{
    // Txn:
    // oldV = read
    // newV = oldV + 1
    // update v = newV if v == oldV
    class MyThread : OTSUnitTestBase
    {
        private volatile int count;
        private readonly int round;
        private readonly String tableName;
        private Thread thread;
        readonly Int64 pk;
        public MyThread(String name, Int64 index)
        {
            tableName = name;
            pk = index;
            round = 100;
            thread = new Thread(new ThreadStart(Run));
        }
        public void Start()
        {
            if (thread != null) { thread.Start(); }
        }
        public void Join()
        {
            thread.Join();
        }
        private void Run()
        {
            for (int i = 0; i < round; ++i)
            {
                Console.WriteLine("Thread:" + i);
                var primaryKey = new PrimaryKey
                {
                    { "PK0", new ColumnValue(pk) }
                };
                var request = new GetRowRequest(tableName, primaryKey);
                var response = OTSClient.GetRow(request);
                var attr = response.Attribute["Col1"];
                long oldIntValue = attr.IntegerValue;
                ColumnValue oldValue = new ColumnValue(oldIntValue);
                ColumnValue newValue = new ColumnValue(oldIntValue + 1);
                RelationalCondition cc = new RelationalCondition("Col1", CompareOperator.EQUAL, oldValue);
                Condition cond = new Condition(RowExistenceExpectation.IGNORE)
                {
                    ColumnCondition = cc
                };
                UpdateOfAttribute updateOfAttributeForPut = new UpdateOfAttribute();
                updateOfAttributeForPut.AddAttributeColumnToPut("Col1", newValue);
                UpdateRowRequest updateReq = new UpdateRowRequest(tableName, cond, primaryKey, updateOfAttributeForPut);
                bool success = true;
                try
                {
                    OTSClient.UpdateRow(updateReq);
                }
                catch (OTSServerException)
                {
                    success = false;
                }
                if (success)
                {
                    ++count;
                }
            }
        }
        public int GetValue() { return count; }
    }

    [TestFixture]
    class ConditionUpdateTest : OTSUnitTestBase
    {
        private void CreateTable(String tableName)
        {
            foreach (var tableItem in OTSClient.ListTable(new ListTableRequest()).TableNames)
            {
                OTSClient.DeleteTable(new DeleteTableRequest(tableItem));
            }
            var primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.Integer }
            };
            var tableMeta = new TableMeta(tableName, primaryKeySchema);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request = new CreateTableRequest(tableMeta, reservedThroughput);
            var response = OTSClient.CreateTable(request);
            WaitForTableReady();
        }
        private bool PutRow(string tableName, Int64 pk, string colName, ColumnValue colValue, IColumnCondition cc)
        {
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(pk) }
            };

            var attribute = new AttributeColumns
            {
                { colName, colValue }
            };
            Condition cond = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = cc
            };

            var request = new PutRowRequest(tableName, cond)
            {
                RowPutChange = new RowPutChange(tableName, primaryKey)
            };

            request.RowPutChange.AddColumns(attribute);

            bool success = true;
            try
            {
                OTSClient.PutRow(request);
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("PutRow fail: {0}", e.ErrorMessage);
                success = false;
            }
            return success;
        }

        public long ReadRow(String tableName, Int64 pk)
        {
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(pk) }
            };
            var request = new GetRowRequest(tableName, primaryKey);
            var response = OTSClient.GetRow(request);
            var attr = response.Attribute["Col1"];
            long value = attr.IntegerValue;
            return value;
        }
        public bool UpdateRow(String tableName, Int64 pk, String colName, ColumnValue colValue, IColumnCondition cond)
        {
            bool success = true;
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(pk) }
            };
            Condition rowCond = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = cond
            };
            UpdateOfAttribute updateOfAttributeForPut = new UpdateOfAttribute();
            updateOfAttributeForPut.AddAttributeColumnToPut(colName, colValue);
            var request = new UpdateRowRequest(tableName, rowCond, primaryKey, updateOfAttributeForPut);
            try
            {
                OTSClient.UpdateRow(request);
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("UpdateRow fail: {0}", e.ErrorMessage);
                success = false;
            }
            return success;
        }

        private bool DeleteRow(String tableName, Int64 pk, IColumnCondition cond)
        {
            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(pk) }
            };
            Condition c = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = cond
            };
            var request = new DeleteRowRequest(tableName, c, primaryKey);
            bool success = true;
            try
            {
                OTSClient.DeleteRow(request);
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("DeleteRow fail:{0}", e.ErrorMessage);
                success = false;
            }
            return success;
        }

        [Test]
        public void TestSingleColumnCondition()
        {
            String tableName = "condition_update_test_table";
            CreateTable(tableName);
            bool success = PutRow(tableName, 1, "Col1", new ColumnValue("Value1"), null);
            Assert.IsTrue(success);

            // put row with condition: col1 != value1
            success = PutRow(tableName, 1, "Col2", new ColumnValue("Value2"),
                new RelationalCondition("Col1",
                    CompareOperator.NOT_EQUAL,
                    new ColumnValue("Value1")));
            Assert.IsFalse(success);

            // put row with condition col1 == value1
            success = PutRow(tableName, 1, "Col2", new ColumnValue("Value2"),
                new RelationalCondition("Col1",
                    CompareOperator.EQUAL,
                    new ColumnValue("Value1")));
            Assert.IsTrue(success);

            // update row with condition: col2 < value1
            success = UpdateRow(tableName, 1, "Col3", new ColumnValue("Value3"),
                new RelationalCondition("Col2",
                    CompareOperator.LESS_THAN,
                    new ColumnValue("Value1")));
            Assert.IsFalse(success);

            // update row with condition: col2 >= value2
            success = UpdateRow(tableName, 1, "Col3", new ColumnValue("Value3"),
                new RelationalCondition("Col2",
                    CompareOperator.GREATER_EQUAL,
                    new ColumnValue("Value2")));
            Assert.IsTrue(success);

            // delete row with condition: col3 <= value2
            success = DeleteRow(tableName, 1,
                    new RelationalCondition("Col3",
                            CompareOperator.LESS_EQUAL,
                            new ColumnValue("Value2")));
            Assert.IsFalse(success);

            // delete row with condition: col3 > value2
            success = DeleteRow(tableName, 1,
                    new RelationalCondition("Col3",
                            CompareOperator.GREATER_THAN,
                            new ColumnValue("Value2")));
            Assert.IsTrue(success);
        }

        [Test]
        public void TestColumnMissing()
        {
            string tableName = "condition_update_test_table";
            CreateTable(tableName);
            bool success = PutRow(tableName, 2, "Col1", new ColumnValue("Value1"), null);
            Assert.IsTrue(success);

            // put row with condition: colX != valueY
            // with passIfMissing == true, this should succeed
            success = PutRow(tableName, 2, "Col2", new ColumnValue("Value2"),
                    new RelationalCondition("ColX",
                            CompareOperator.NOT_EQUAL,
                            new ColumnValue("ValueY")));
            Assert.IsTrue(success);

            // put row with condition: colX != valueY
            // with passIfMissing == false, this should fail
            RelationalCondition cond = new RelationalCondition("ColX",
                    CompareOperator.NOT_EQUAL,
                    new ColumnValue("ValueY"))
            {
                PassIfMissing = false
            };
            success = PutRow(tableName, 2, "Col2", new ColumnValue("Value2"), cond);
            Assert.IsFalse(success);
        }

        [Test]
        public void TestCompositeCondition()
        {
            String tableName = "condition_update_test_table";
            CreateTable(tableName);

            bool success = PutRow(tableName, 3, "Col1", new ColumnValue("Value1"), null);
            Assert.IsTrue(success);

            success = UpdateRow(tableName, 3, "Col2", new ColumnValue("Value2"), null);
            Assert.IsTrue(success);

            // update with condition:
            // col1 == value2 OR col2 == value1
            CompositeCondition cond = new CompositeCondition(LogicOperator.OR);
            cond.AddCondition(new RelationalCondition(
                    "Col1",
                    CompareOperator.EQUAL,
                    new ColumnValue("Value2")))
                .AddCondition(new RelationalCondition(
                    "Col2",
                    CompareOperator.EQUAL,
                    new ColumnValue("Value1")));
            success = UpdateRow(tableName, 3, "Col3", new ColumnValue("Value3"), cond);
            Assert.IsFalse(success);

            // update with condition
            // Not col1 == value2
            cond = new CompositeCondition(LogicOperator.NOT);
            cond.AddCondition(new RelationalCondition(
                "Col1",
                CompareOperator.EQUAL,
                new ColumnValue("Value2")));
            success = UpdateRow(tableName, 3, "Col3", new ColumnValue("Value3"), cond);
            Assert.IsTrue(success);

            // delete with condition
            // col1 == valueX  OR  (col2 == value2 AND col3 == value3)
            cond = new CompositeCondition(LogicOperator.OR);
            cond.AddCondition(new RelationalCondition(
                "Col1",
                CompareOperator.EQUAL,
                new ColumnValue("ValueX")));
            CompositeCondition cond2 = new CompositeCondition(LogicOperator.AND);
            cond2.AddCondition(new RelationalCondition(
                "Col2",
                CompareOperator.EQUAL,
                new ColumnValue("Value2")))
                .AddCondition(new RelationalCondition(
                    "Col3",
                    CompareOperator.EQUAL,
                    new ColumnValue("Value3")));
            cond.AddCondition(cond2);
            success = DeleteRow(tableName, 3, cond);
            Assert.IsTrue(success);
        }

        [Test]
        public void TestLimits()
        {
            String tableName = "condition_update_test_table";
            CreateTable(tableName);
            // column condition count <= 10
            CompositeCondition cond = new CompositeCondition(LogicOperator.OR);
            cond.AddCondition(new RelationalCondition(
                        "ColX1", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX2", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX3", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX4", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX5", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX6", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX7", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX8", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                        "ColX9", CompareOperator.EQUAL,
                        new ColumnValue("ValueX")));
            bool success = PutRow(tableName, 4, "Col1", new ColumnValue("Value1"), cond);
            Assert.IsTrue(success);

            cond.AddCondition(new RelationalCondition(
                "ColX10", CompareOperator.EQUAL,
                new ColumnValue("ValueX")));
            success = PutRow(tableName, 4, "Col1", new ColumnValue("Value1"), cond);
            Assert.IsFalse(success);

            // invalid column name in column condition
            cond = new CompositeCondition(LogicOperator.AND);
            cond.AddCondition(new RelationalCondition(
                    "#Col", CompareOperator.EQUAL,
                    new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                    "ColX9", CompareOperator.EQUAL,
                    new ColumnValue("ValueX")));
            success = PutRow(tableName, 4, "Col1", new ColumnValue("Value1"), cond);
            Assert.IsFalse(success);

            // invalid column value in column condition
            cond = new CompositeCondition(LogicOperator.AND);
            cond.AddCondition(new RelationalCondition(
                    "ColX9", CompareOperator.EQUAL,
                    new ColumnValue("ValueX")))
                .AddCondition(new RelationalCondition(
                    "ColX9", CompareOperator.EQUAL,
                    new ColumnValue("ValueX")));
            Assert.IsFalse(success);
        }

        /**
        [Test]
        public void TestTransactionalUpdate()
        {
            String tableName = "condition_update_test_table";
            CreateTable(tableName);

            bool success = PutRow(tableName, 5, "Col1", new ColumnValue(0), null);
            Assert.IsTrue(success);

            int threadNum = 100;
            List<MyThread> threadList = new List<MyThread>();
            for(int j = 0; j < threadNum; ++j)
            {
                MyThread mythread = new MyThread(tableName, 5);
                threadList.Add(mythread);
                mythread.Start();
            }

            foreach(var tmpThread in threadList)
            {
                tmpThread.Join();
            }

            int total = 0;
            foreach (var tmp in threadList) {
                total += tmp.GetValue();
            }

            long v = ReadRow(tableName, 5);
            Assert.AreEqual(total, v);
        }
        **/
        [Test]
        public void TestBatchWriteRow()
        {
            String tableName = "condition_update_test_table";
            CreateTable(tableName);

            bool success = PutRow(tableName, 20, "Col1", new ColumnValue("Value20"), null);
            Assert.IsTrue(success);
            success = PutRow(tableName, 21, "Col1", new ColumnValue("Value21"), null);
            Assert.IsTrue(success);
            success = PutRow(tableName, 22, "Col1", new ColumnValue("Value22"), null);
            Assert.IsTrue(success);

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(20) }
            };
            var attribute = new AttributeColumns
            {
                { "Col2", new ColumnValue("Value2") }
            };
            Condition cond1 = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = new RelationalCondition(
                    "Col1", CompareOperator.NOT_EQUAL,
                    new ColumnValue("Value20"))
            };

            var updatePrimaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(21) }
            };
            UpdateOfAttribute update2 = new UpdateOfAttribute();
            update2.AddAttributeColumnToPut("Col3", new ColumnValue("Value3"));
            Condition cond2 = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = new RelationalCondition(
                    "Col1", CompareOperator.EQUAL,
                    new ColumnValue("Value21"))
            };

            var deletePrimaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue(22) }
            };
            Condition cond3 = new Condition(RowExistenceExpectation.IGNORE)
            {
                ColumnCondition = new RelationalCondition(
                    "Col1", CompareOperator.GREATER_THAN,
                    new ColumnValue("Value22"))
            };

            var batchWriteRequest = new BatchWriteRowRequest();
            RowChanges changes = new RowChanges(tableName);
            changes.AddPut(cond1, primaryKey, attribute);
            changes.AddUpdate(cond2, updatePrimaryKey, update2);
            changes.AddDelete(cond3, deletePrimaryKey);
            batchWriteRequest.Add(tableName, changes);

            var response = OTSClient.BatchWriteRow(batchWriteRequest);
            var tables = response.TableRespones;
            Assert.AreEqual(1, tables.Count);
            var rows = tables[tableName];
            Assert.IsFalse(rows.Responses[0].IsOK);
        }
    }
}
