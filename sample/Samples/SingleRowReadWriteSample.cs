using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.DataModel.ConditionalUpdate;
using System.Threading.Tasks;

namespace Aliyun.OTS.Samples
{
    public static class SingleRowReadWriteSample
    {

        private static readonly string TableName = "singleRowReadWriteSample";

        private static void PrepareTable()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(TableName)) {
                return;
            }


            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
            {
                { "pk0", ColumnValueType.Integer },
                { "pk1", ColumnValueType.String }
            };
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

        }

        public static void PutRow()
        {
            Console.WriteLine("Start put row...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { "pk0", new ColumnValue(0) },
                { "pk1", new ColumnValue("abc") }
            };

            // 定义要写入该行的属性列
            AttributeColumns attribute = new AttributeColumns
            {
                { "col0", new ColumnValue(0) },
                { "col1", new ColumnValue("a") },
                { "col2", new ColumnValue(true) }
            };
            PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            otsClient.PutRow(request);
            Console.WriteLine("Put row succeed.");
        }

        public static void PutRowAsync()
        {
            Console.WriteLine("Start put row async...");
            PrepareTable();
            OTSClient TabeStoreClient = Config.GetClient();

            try
            {
                var putRowTaskList = new List<Task<PutRowResponse>>();
                for (int i = 0; i < 100; i++)
                {
                    // 定义行的主键，必须与创建表时的TableMeta中定义的一致
                    var primaryKey = new PrimaryKey
                    {
                        { "pk0", new ColumnValue(i) },
                        { "pk1", new ColumnValue("abc") }
                    };

                    // 定义要写入改行的属性列
                    var attribute = new AttributeColumns
                    {
                        { "col0", new ColumnValue(i) },
                        { "col1", new ColumnValue("a") },
                        { "col2", new ColumnValue(true) }
                    };

                    var request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE),
                                                    primaryKey, attribute);

                    putRowTaskList.Add(TabeStoreClient.PutRowAsync(request));
                }

                foreach (var task in putRowTaskList)
                {
                    task.Wait();
                    Console.WriteLine("consumed read:{0}, write:{1}", task.Result.ConsumedCapacityUnit.Read,
                                       task.Result.ConsumedCapacityUnit.Write);
                }

                Console.WriteLine("Put row async succeeded.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Put row async failed. exception:{0}", ex.Message);
            }
        }

        public static void UpdateRow()
        {
            Console.WriteLine("Start update row...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { "pk0", new ColumnValue(0) },
                { "pk1", new ColumnValue("abc") }
            };

            // 定义要写入该行的属性列
            UpdateOfAttribute attribute = new UpdateOfAttribute();
            attribute.AddAttributeColumnToPut("col0", new ColumnValue(0));
            attribute.AddAttributeColumnToPut("col1", new ColumnValue("b")); // 将原先的值'a'改为'b'
            attribute.AddAttributeColumnToPut("col2", new ColumnValue(true));
            UpdateRowRequest request = new UpdateRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            otsClient.UpdateRow(request);
            Console.WriteLine("Update row succeed.");
        }

        private static string PrintColumnValue(ColumnValue value)
        {
            switch(value.Type)
            {
                case ColumnValueType.String: return value.StringValue;
                case ColumnValueType.Integer: return value.IntegerValue.ToString();
                case ColumnValueType.Boolean: return value.BooleanValue.ToString();
                case ColumnValueType.Double: return value.DoubleValue.ToString();
                case ColumnValueType.Binary: return value.BinaryValue.ToString();
            }

            throw new Exception("Unknow type.");
        }

        public static void GetRow()
        {
            Console.WriteLine("Start get row...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { "pk0", new ColumnValue(0) },
                { "pk1", new ColumnValue("abc") }
            };

            GetRowRequest request = new GetRowRequest(TableName, primaryKey); // 未指定读哪列，默认读整行
            GetRowResponse response = otsClient.GetRow(request);
            PrimaryKey primaryKeyRead = response.PrimaryKey;
            AttributeColumns attributesRead = response.Attribute;

            Console.WriteLine("Primary key read: ");
            foreach(KeyValuePair<string, ColumnValue> entry in primaryKeyRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Attributes read: ");
            foreach (KeyValuePair<string, ColumnValue> entry in attributesRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Get row succeed.");
        }
        
        public static void deleteRow()
        {
            Console.WriteLine("Start delete row");
            PrepareTable();

            OTSClient otsClient = Config.GetClient();
            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { "pk0", new ColumnValue(0) },
                { "pk1", new ColumnValue("abc") }
            };

            DeleteRowRequest request = new DeleteRowRequest(TableName,new Condition(RowExistenceExpectation.EXPECT_EXIST),primaryKey);
            otsClient.DeleteRow(request);
            Console.WriteLine("end  delete row");
        }
         
        public static void GetRowWithFilter()
        {
            Console.WriteLine("Start get row with filter ...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { "pk0", new ColumnValue(0) },
                { "pk1", new ColumnValue("abc") }
            };

            var rowQueryCriteria = new SingleRowQueryCriteria(TableName)
            {
                RowPrimaryKey = primaryKey
            };

            // 只返回col0的值等于5的行或者col1不等于ff的行
            var filter1 = new RelationalCondition("col0",
                    CompareOperator.EQUAL,
                    new ColumnValue(5));

            var filter2 = new RelationalCondition("col1", CompareOperator.NOT_EQUAL, new ColumnValue("ff"));

            var filter = new CompositeCondition(LogicOperator.OR);
            filter.AddCondition(filter1);
            filter.AddCondition(filter2);

            rowQueryCriteria.Filter = filter.ToFilter();
            rowQueryCriteria.AddColumnsToGet("col0");
            rowQueryCriteria.AddColumnsToGet("col1");

            GetRowRequest request = new GetRowRequest(rowQueryCriteria); 

            // 查询
            GetRowResponse response = otsClient.GetRow(request);
            PrimaryKey primaryKeyRead = response.PrimaryKey;
            AttributeColumns attributesRead = response.Attribute;

            Console.WriteLine("Primary key read: ");
            foreach (KeyValuePair<string, ColumnValue> entry in primaryKeyRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Attributes read: ");
            foreach (KeyValuePair<string, ColumnValue> entry in attributesRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Get row with filter succeed.");
        }
    }
}
