using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.Samples.Samples
{
    /// <summary>
    /// 原子自增示例
    /// 可以指定某一列为原子自增列，每次按照指定的数值进行累加。
    /// </summary>
    public class AtomicIncrementSample
    {
        private static readonly string TableName = "AtomicIncrementSample";

        private static readonly string Pk1 = "Pk1";
        private static readonly string Pk2 = "Pk2";
        private static readonly string IncrementCol = "IncrementCol";


        static void Main(string[] args)
        {
            Console.WriteLine("AtomicIncrementSample");

            //准备表
            PrepareTable();

            //新增一行，设置IncrementCol这个属性列的初始值为0
            PutRow();

            //对IncrementCol执行原子自增，10次，每次加1
            for (int i = 0; i < 10; i++)
            {
                Increment(1);
            }

            //获取自增后的列
            GetRow();

            Console.ReadLine();

        }


        private static void PrepareTable()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(TableName))
            {
                return;
            }

            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
            {
                { Pk1, ColumnValueType.Integer },
                { Pk2, ColumnValueType.String }
            };
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

        }

        public static void PutRow()
        {
            Console.WriteLine("Start put row...");
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { Pk1, new ColumnValue(0) },
                { Pk2, new ColumnValue("abc") }
            };

            // 定义要写入该行的属性列
            AttributeColumns attribute = new AttributeColumns
            {
                { IncrementCol, new ColumnValue(0) }
            };
            PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            otsClient.PutRow(request);
            Console.WriteLine("Put row succeed.");
        }


        /// <summary>
        /// 更新行的时候，指定某一列为原子自增列，并对这一列进行原子自增
        /// </summary>
        public static void Increment(int incrementValue)
        {
            Console.WriteLine("Start set increment column...");
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { Pk1, new ColumnValue(0) },
                { Pk2, new ColumnValue("abc") }
            };
            RowUpdateChange rowUpdateChange = new RowUpdateChange(TableName, primaryKey);
            rowUpdateChange.ReturnType = ReturnType.RT_AFTER_MODIFY;
            rowUpdateChange.ReturnColumnNames = new List<string>() { IncrementCol};
            //设置一个原子自增列，这一列从0开始自增，每次增增加1。
            rowUpdateChange.Increment(new Column(IncrementCol, new ColumnValue(incrementValue)));

            UpdateRowRequest updateRowRequest = new UpdateRowRequest(rowUpdateChange);

            var response = otsClient.UpdateRow(updateRowRequest);
            Console.WriteLine("set Increment column succeed，Increment result:" + response.Row.GetColumns()[0].Value);
        }


        public static void GetRow()
        {
            Console.WriteLine("Start get row...");
            PrepareTable();
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { Pk1, new ColumnValue(0) },
                { Pk2, new ColumnValue("abc") }
            };

            GetRowRequest request = new GetRowRequest(TableName, primaryKey); // 未指定读哪列，默认读整行
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

            Console.WriteLine("Get row succeed.");
        }



        private static string PrintColumnValue(ColumnValue value)
        {
            switch (value.Type)
            {
                case ColumnValueType.String: return value.StringValue;
                case ColumnValueType.Integer: return value.IntegerValue.ToString();
                case ColumnValueType.Boolean: return value.BooleanValue.ToString();
                case ColumnValueType.Double: return value.DoubleValue.ToString();
                case ColumnValueType.Binary: return value.BinaryValue.ToString();
            }

            throw new Exception("Unknow type.");
        }


    }
}
