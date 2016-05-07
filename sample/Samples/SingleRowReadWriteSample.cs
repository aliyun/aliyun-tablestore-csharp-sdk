using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Samples
{
    public static class SingleRowReadWriteSample
    {

        private static string TableName = "singleRowReadWriteSample";

        private static void PrepareTable()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(TableName)) {
                return;
            }


            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("pk0", ColumnValueType.Integer);
            primaryKeySchema.Add("pk1", ColumnValueType.String);
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

        }

        public static void PutRow()
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
            PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            otsClient.PutRow(request);
            Console.WriteLine("Put row succeed.");
        }

        public static void UpdateRow()
        {
            Console.WriteLine("Start update row...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(0));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            // 定义要写入改行的属性列
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
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(0));
            primaryKey.Add("pk1", new ColumnValue("abc"));

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
         
        public static void GetRowWithFilter()
        {
            Console.WriteLine("Start get row with filter ...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey();
            primaryKey.Add("pk0", new ColumnValue(0));
            primaryKey.Add("pk1", new ColumnValue("abc"));

            var rowQueryCriteria = new SingleRowQueryCriteria(TableName);
            rowQueryCriteria.RowPrimaryKey = primaryKey;

            // 只返回col0的值等于5的行
            var condition = new RelationalCondition("col0",
                    RelationalCondition.CompareOperator.EQUAL,
                    new ColumnValue(5));

            rowQueryCriteria.Filter = condition;
            rowQueryCriteria.AddColumnsToGet("col0");

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
