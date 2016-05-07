using NUnit.Framework;
using System;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class ConditionUpdateTest:OTSUnitTestBase
    {
        private static String COLUMN_GID_NAME = "gid";
        private static String COLUMN_UID_NAME = "uid";
        private static String COLUMN_NAME_NAME = "name";
        private static String COLUMN_ADDRESS_NAME = "address";
        private static String COLUMN_AGE_NAME = "age";
        private static String COLUMN_MOBILE_NAME = "mobile";

        private void CreateTable(OTSClient client, String tableName)
        {
            foreach (var tableItem in client.ListTable(new ListTableRequest()).TableNames)
            {
                client.DeleteTable(new DeleteTableRequest(tableItem));
            }
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add(COLUMN_GID_NAME, ColumnValueType.Integer);
            primaryKeySchema.Add(COLUMN_UID_NAME, ColumnValueType.Integer);
            var tableMeta = new TableMeta(tableName, primaryKeySchema);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request = new CreateTableRequest(tableMeta, reservedThroughput);
            var response = OTSClient.CreateTable(request);
            // 创建表只是提交请求，OTS创建表需要一段时间，这里是简单的sleep，请根据实际逻辑修改
            WaitForTableReady();
        }

        private void PutRow(OTSClient client, String tableName)
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add(COLUMN_GID_NAME, new ColumnValue(1));
            primaryKey.Add(COLUMN_UID_NAME, new ColumnValue(101));

            var attribute = new AttributeColumns();
            attribute.Add(COLUMN_NAME_NAME, new ColumnValue("张三"));
            attribute.Add(COLUMN_MOBILE_NAME, new ColumnValue(111111111));
            attribute.Add(COLUMN_ADDRESS_NAME, new ColumnValue("中国A地"));
            attribute.Add(COLUMN_AGE_NAME, new ColumnValue(20));

            Condition cond = new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST);
            var request = new PutRowRequest(tableName, cond, primaryKey, attribute);
            try
            {
                client.PutRow(request);
                Console.WriteLine("PutRow success");
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("PutRow fail: {0}", e.ErrorMessage);
            }
        }
        private void UpdateRow(OTSClient client, String tableName, ColumnCondition cond)
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add(COLUMN_GID_NAME, new ColumnValue(1));
            primaryKey.Add(COLUMN_UID_NAME, new ColumnValue(101));

            UpdateOfAttribute updateOfAttributeForPut = new UpdateOfAttribute();
            updateOfAttributeForPut.AddAttributeColumnToPut(COLUMN_NAME_NAME, new ColumnValue("张三"));
            updateOfAttributeForPut.AddAttributeColumnToPut(COLUMN_ADDRESS_NAME, new ColumnValue("中国B地"));
            updateOfAttributeForPut.AddAttributeColumnToDelete(COLUMN_MOBILE_NAME);
            updateOfAttributeForPut.AddAttributeColumnToDelete(COLUMN_AGE_NAME);

            Condition condition = new Condition(RowExistenceExpectation.IGNORE);
            condition.ColumnCondition = cond;

            var request = new UpdateRowRequest(tableName, condition, primaryKey, updateOfAttributeForPut);
            try
            {
                client.UpdateRow(request);
                Console.WriteLine("UpdateRow success");
            }
            catch (OTSServerException e)
            {
                //服务端异常
                Console.WriteLine("操作失败:{0}", e.ErrorMessage);
                Console.WriteLine("请求ID:{0}", e.RequestID);
            }
            catch(OTSClientException e)
            {
                //可能是网络不好或者返回结果有问题
                Console.WriteLine("请求失败:{0}", e.ErrorMessage);
            }
        }
        private void DeleteRow(OTSClient client, String tableName)
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add(COLUMN_GID_NAME, new ColumnValue(1));
            primaryKey.Add(COLUMN_UID_NAME, new ColumnValue(101));
            Condition condition = new Condition(RowExistenceExpectation.IGNORE);
            DeleteRowRequest req = new DeleteRowRequest(tableName, condition, primaryKey);
            client.DeleteRow(req);
            Console.WriteLine("DeleteRow success");
        }
        private void DeleteTable(OTSClient client, String tableName)
        {
            DeleteTableRequest req = new DeleteTableRequest(tableName);
            client.DeleteTable(req);
            Console.WriteLine("DeleteTable success");
        }
        private void GetRow(OTSClient client, String tableName)
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add(COLUMN_GID_NAME, new ColumnValue(1));
            primaryKey.Add(COLUMN_UID_NAME, new ColumnValue(101));
            var request = new GetRowRequest(tableName, primaryKey);
            var response = OTSClient.GetRow(request);
            String name = response.Attribute[COLUMN_NAME_NAME].StringValue;
            String addr = response.Attribute[COLUMN_ADDRESS_NAME].StringValue;
            long age = response.Attribute[COLUMN_AGE_NAME].IntegerValue;
            Console.WriteLine("本次读取name信息:{0}", name);
            Console.WriteLine("本次读取addr信息:{0}", addr);
            Console.WriteLine("本次读取age信息: {0}", age);
        }

        [Test]
        public void ConditionUpdateExampleTest()
        {
            var otsClient = OTSClient;
            String tableName = "condition_update_test_example";
            try
            {
                CreateTable(otsClient, tableName);

                PutRow(otsClient, tableName);
                GetRow(otsClient, tableName);

                // 设置update condition：年龄< 20岁
                // UpdateRow应该失败
                ColumnCondition cond = new RelationalCondition(COLUMN_AGE_NAME, RelationalCondition.CompareOperator.LESS_THAN, new ColumnValue(20));
                UpdateRow(otsClient, tableName, cond);

                //设置update condition: 年龄 >= 20岁 并且 地址是“中国A地“
                //UpdateRow应该成功
                cond = new CompositeCondition(CompositeCondition.LogicOperator.AND)
                    .AddCondition(new RelationalCondition(
                            COLUMN_AGE_NAME, RelationalCondition.CompareOperator.GREATER_THAN,
                            new ColumnValue(20)))
                    .AddCondition(new RelationalCondition(
                            COLUMN_ADDRESS_NAME, RelationalCondition.CompareOperator.EQUAL,
                            new ColumnValue("中国A地")));
                UpdateRow(otsClient, tableName, cond);
                GetRow(otsClient, tableName);

                DeleteRow(otsClient, tableName);
            }
            catch(OTSServerException e)
            {
                Console.WriteLine("服务失败:{0}", e.ErrorMessage);
                Console.WriteLine("Request ID:{0}", e.RequestID);
            }
            catch(OTSClientException e)
            {
                Console.WriteLine("OTSClientException:{0}", e.ErrorMessage);
            }
            catch(Exception e)
            {
                Console.WriteLine("Exception:{0}", e.Message);
            }
            finally
            {
                try
                {
                    DeleteTable(otsClient, tableName);
                }catch(OTSServerException e)
                {
                    Console.WriteLine("删表失败:{0}", e.ErrorMessage);
                }
                catch(OTSClientException e)
                {
                    Console.WriteLine("删表请求失败:{0}", e.ErrorMessage);
                }
            }
        }
    }
}
