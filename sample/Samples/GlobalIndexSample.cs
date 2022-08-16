using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.Samples.Samples
{
    public class GlobalIndexSample
    {
        private static readonly string TableName = "tableWithGlobalIndexSample";
        private static readonly string IndexName = "tableWithGlobalIndexSampleIndex";
        private static readonly string IndexName2 = "tableWithGlobalIndexSampleIndex2";

        private static readonly string Pk1 = "Pk1";
        private static readonly string Pk2 = "Pk2";
        private static readonly string Col1 = "Col1";
        private static readonly string Col2 = "Col2";


        static void Main(string[] args)
        {
            Console.WriteLine("GlobalIndexSample");

            CreateTableWithGlobalIndex();

            CreateGlobalIndex();

            PutRow();

            GetRangeFromIndexTable();

            DeleteGlobalIndex();

            DeleteTable();

            Console.ReadLine();

        }

        /// <summary>
        /// 创建一个带二级索引的表
        /// </summary>
        public static void CreateTableWithGlobalIndex()
        {
            //建主表，两列Pk:Pk1、Pk2。 预定义列:Col1、Col2。
            //建索引表，索引表中Col1放Pk0
            OTSClient otsClient = Config.GetClient();

            Console.WriteLine("Start create table with globalIndex...");
            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
                {
                    { Pk1, ColumnValueType.String },
                    { Pk2, ColumnValueType.String }
                };
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);
            tableMeta.DefinedColumnSchema = new DefinedColumnSchema {
                   { Col1, DefinedColumnType.STRING},
                   { Col2,DefinedColumnType.STRING}
                };

            IndexMeta indexMeta = new IndexMeta(IndexName);
            indexMeta.PrimaryKey = new List<string>() { Col1 };
            indexMeta.DefinedColumns = new List<string>() { Col2 };
            //indexMeta.IndexType = IndexType.IT_GLOBAL_INDEX;
            //indexMeta.IndexUpdateModel = IndexUpdateMode.IUM_ASYNC_INDEX;

            List<IndexMeta> indexMetas = new List<IndexMeta>() { };
            indexMetas.Add(indexMeta);

            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput, indexMetas);
            otsClient.CreateTable(request);

            Console.WriteLine("Table is created: " + TableName);

        }

        /// <summary>
        /// 单独在表上再创建一个索引表2
        /// </summary>
        public static void CreateGlobalIndex()
        {
            OTSClient otsClient = Config.GetClient();

            Console.WriteLine("Start create globalIndex...");

            IndexMeta indexMeta = new IndexMeta(IndexName2);
            indexMeta.PrimaryKey = new List<string>() { Col2 };
            indexMeta.DefinedColumns = new List<string>() { Pk1 };


            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateGlobalIndexRequest request = new CreateGlobalIndexRequest(TableName, indexMeta);
            otsClient.CreateGlobalIndex(request);

            Console.WriteLine("Global Index is created,tableName: " + TableName + ",IndexName:" + IndexName2);

        }

        /// <summary>
        /// 向主表中写入数据（自动同步到索引表）
        /// </summary>
        public static void PutRow()
        {
            Console.WriteLine("Start put row...");
            OTSClient otsClient = Config.GetClient();
            PrimaryKey primaryKey = new PrimaryKey
            {
                { Pk1, new ColumnValue("abc") },
                { Pk2, new ColumnValue("edf") }
            };

            // 定义要写入改行的属性列
            AttributeColumns attribute = new AttributeColumns
            {
                { Col1, new ColumnValue("Col1Value") },
                { Col2, new ColumnValue("Col2Value") }
            };
            PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

            otsClient.PutRow(request);
            Console.WriteLine("Put row succeed.");
        }

        /// <summary>
        /// 从索引表中读取数据
        /// </summary>
        public static void GetRangeFromIndexTable()
        {
            Console.WriteLine("Start getRange from index...");
            OTSClient otsClient = Config.GetClient();
            // 指定第一主键Col1的值，进行扫描
            PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey
            {
                { Col1, new ColumnValue("Col1Value") },
                { Pk1,  ColumnValue.INF_MIN },
                { Pk2, ColumnValue.INF_MIN }
            };

            PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey
            {
                { Col1, new ColumnValue("Col1Value") },
                { Pk1,  ColumnValue.INF_MAX },
                { Pk2, ColumnValue.INF_MAX }
            };

            GetRangeRequest request = new GetRangeRequest(IndexName, GetRangeDirection.Forward, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey);

            GetRangeResponse response = otsClient.GetRange(request);
            IList<Row> rows = response.RowDataList;
            PrimaryKey nextStartPrimaryKey = response.NextPrimaryKey;
            while (nextStartPrimaryKey != null)
            {
                request = new GetRangeRequest(TableName, GetRangeDirection.Forward, nextStartPrimaryKey, exclusiveEndPrimaryKey);
                response = otsClient.GetRange(request);
                nextStartPrimaryKey = response.NextPrimaryKey;
                foreach (var row in response.RowDataList)
                {
                    rows.Add(row);
                }
            }

            foreach (var row in rows)
            {
                PrintRow(row);
            }

            Console.WriteLine("TotalRowsRead: " + rows.Count);
        }


        /// <summary>
        /// 删除索引表1、2
        /// </summary>
        public static void DeleteGlobalIndex()
        {
            OTSClient otsClient = Config.GetClient();

            Console.WriteLine("Start delete globalIndex...");

            DeleteGlobalIndexRequest request = new DeleteGlobalIndexRequest(TableName, IndexName);
            otsClient.DeleteGlobalIndex(request);

            DeleteGlobalIndexRequest request2 = new DeleteGlobalIndexRequest(TableName, IndexName2);
            otsClient.DeleteGlobalIndex(request2);

            Console.WriteLine("Global Index is deleted,tableName: " + TableName + ",IndexName:" + IndexName + "," + IndexName2);

        }

        public static void DeleteTable()
        {
            OTSClient otsClient = Config.GetClient();
            Console.WriteLine("Start delete table...");
            DeleteTableRequest request = new DeleteTableRequest(TableName);
            otsClient.DeleteTable(request);
            Console.WriteLine("Table is deleted.");
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

        private static void PrintRow(Row row)
        {
            Console.WriteLine("-----------------");

            foreach (KeyValuePair<string, ColumnValue> entry in row.GetPrimaryKey())
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            foreach (Column entry in row.GetColumns())
            {
                Console.WriteLine(entry.Name + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("-----------------");
        }
    }
}
