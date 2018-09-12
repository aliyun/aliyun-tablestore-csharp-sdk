using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Samples
{
    public static class ConditionUpdateSample 
    {

        private static string tableName = "condition_update_sample";

        private static void PrepareTable()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(tableName)) {
                return;
            }

            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("pk0", ColumnValueType.Integer);
            primaryKeySchema.Add("pk1", ColumnValueType.String);
            TableMeta tableMeta = new TableMeta(tableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);
        }

        public static void ConditionPutRow()
        {
            Console.WriteLine("Start put row...");

            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(0));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            // 定义要写入改行的属性列
            AttributeColumns attribute = new AttributeColumns();
            attribute.Add("col0", new ColumnValue(0));
            attribute.Add("col1", new ColumnValue("a"));
            attribute.Add("col2", new ColumnValue(true));

            PutRowRequest request = new PutRowRequest(tableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            // 不带condition时put row，预期成功
            try
            {
                otsClient.PutRow(request);

                Console.WriteLine("Put row succeeded.");
            } catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }

            // 当col0列的值不等于5的时候，允许再次put row，覆盖掉原值，预期成功
            try
            {
                request.Condition.ColumnCondition = new RelationalCondition("col0",
                                                    CompareOperator.NOT_EQUAL,
                                                    new ColumnValue(5));
                otsClient.PutRow(request);

                Console.WriteLine("Put row succeeded.");
            } catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }

            // 当col0列的值等于5的时候，允许再次put row，覆盖掉原值，预期失败
            try
            {
                // 新增条件：col0列的值等于5
                request.Condition.ColumnCondition = new RelationalCondition("col0",
                                                    CompareOperator.EQUAL,
                                                    new ColumnValue(5));
                otsClient.PutRow(request);

                Console.WriteLine("Put row succeeded.");
            }
            catch (OTSServerException)
            {
                // 由于条件不满足，抛出OTSServerException
                Console.WriteLine("Put row failed  because condition check failed. but expected");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }
        }

        public static void ConditionUpdateRow()
        {
            Console.WriteLine("Start update row...");

            PrepareTable();
            var otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            var primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(1));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            // 定义要写入改行的属性列
            var attribute = new AttributeColumns();
            attribute.Add("col0", new ColumnValue(1));
            attribute.Add("col1", new ColumnValue("a"));
            attribute.Add("col2", new ColumnValue(true));

            var request = new PutRowRequest(tableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            // 新创建一行数据
            try
            {
                otsClient.PutRow(request);

                Console.WriteLine("Put row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }


            // 当col0不等于5，且col1等于'a'时，允许修改，否则不允许修改
            try
            {
                // 构造condition
                var cond1 = new RelationalCondition("col0",
                                                    CompareOperator.NOT_EQUAL,
                                                    new ColumnValue(5));
                var cond2 = new RelationalCondition("col1", CompareOperator.EQUAL,
                                                    new ColumnValue("a"));
                var columenCondition = new CompositeCondition(LogicOperator.AND);
                columenCondition.AddCondition(cond1);
                columenCondition.AddCondition(cond2);

                var condition = new Condition(RowExistenceExpectation.IGNORE);
                condition.ColumnCondition = columenCondition;

                // 构造更新请求
                var updateOfAttribute = new UpdateOfAttribute();
                updateOfAttribute.AddAttributeColumnToPut("col2", new ColumnValue(false));
                var updateRowRequest = new UpdateRowRequest(tableName, condition, primaryKey, updateOfAttribute);

                // 更新数据
                otsClient.UpdateRow(updateRowRequest);

                // 更新成功
                Console.WriteLine("Update row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update row failed. error:{0}", ex.Message);
            }
        }

        public static void ConditionDeleteRow() 
        {
            Console.WriteLine("Start delete row...");

            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(2));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            // 定义要写入改行的属性列
            AttributeColumns attribute = new AttributeColumns();
            attribute.Add("col0", new ColumnValue(2));
            attribute.Add("col1", new ColumnValue("a"));
            attribute.Add("col2", new ColumnValue(true));

            PutRowRequest putRequest = new PutRowRequest(tableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            // 新创建一行数据
            try
            {
                otsClient.PutRow(putRequest);

                Console.WriteLine("Put row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }

            // 当col2列的值等于true的时候，允许删除
            try
            {
                // 构造条件语句：col2列的值等于true
                var condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
                condition.ColumnCondition = new RelationalCondition("col2",
                                            CompareOperator.EQUAL,
                                            new ColumnValue(true));

                // 构造删除请求
                var deleteRequest = new DeleteRowRequest(tableName, condition, primaryKey);

                // 删除满足条件的特定行
                otsClient.DeleteRow(deleteRequest);

                Console.WriteLine("Delete row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Delete row failed. error:{0}", ex.Message);
            }

            Console.WriteLine("Delete row succeeded.");
        }

        public static void ConditionBatchWriteRow() 
        {
            Console.WriteLine("Start batch write row...");

            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(3));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            // 定义要写入改行的属性列
            AttributeColumns attribute = new AttributeColumns();
            attribute.Add("col0", new ColumnValue(0));
            attribute.Add("col1", new ColumnValue("a"));
            attribute.Add("col2", new ColumnValue(true));

            PutRowRequest request = new PutRowRequest(tableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            // 新创建一行数据
            try
            {
                otsClient.PutRow(request);

                Console.WriteLine("Put row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Put row failed. error:{0}", ex.Message);
            }

            // 当col0列的值不等于5的时候，允许再次put row，覆盖掉原值，预期成功
            try
            {
                // 构造条件语句：col0列的值不等于5
                var condition = new Condition(RowExistenceExpectation.IGNORE);
                condition.ColumnCondition = new RelationalCondition("col0",
                                            CompareOperator.NOT_EQUAL,
                                            new ColumnValue(5));

                // 构造col2列的值
                var attr1 = new AttributeColumns();
                attr1.Add("col2", new ColumnValue(false));

                // 构造批量写请求
                var rowChange = new RowChanges(tableName);
                rowChange.AddPut(condition, primaryKey, attr1);

                var batchWriteRequest = new BatchWriteRowRequest();
                batchWriteRequest.Add(tableName, rowChange);

                // 批量写数据
                otsClient.BatchWriteRow(batchWriteRequest);

                Console.WriteLine("Batch write row succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Batch write row failed. error:{0}", ex.Message);
            }
        }
    }
}
