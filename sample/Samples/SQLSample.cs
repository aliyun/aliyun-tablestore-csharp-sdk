using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.SQL;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;


namespace Aliyun.OTS.Samples.Samples
{
    public class SQLSample
    {
        private static readonly string TableName = "SearchIndexSampleTable";
        private static readonly string Pk0 = "pk0";
        private static readonly string Pk1 = "pk1";
        private static readonly string Long_type_col = "Long_type_col";
        private static readonly string Text_type_col = "Text_type_col";
        private static readonly string Keyword_type_col = "Keyword_type_col";
        private static readonly string Date_type_col = "Data_type_col";
        private static readonly string Geo_type_col = "Geo_type_col";
        private static readonly string Virtual_col_Text = "Virtual_col_Text";

        static void Main(string[] args)
        {
            Console.WriteLine("\n start SQL Sample...");

            OTSClient otsClient = Config.GetClient();

            //创建一张TableStore表
            {
                Console.WriteLine("\n Start create table...");

                PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
                {
                    { Pk0, ColumnValueType.Integer },
                    { Pk1, ColumnValueType.String }
                };

                DefinedColumnSchema definedColumnSchema = new DefinedColumnSchema
                {
                    { Long_type_col, DefinedColumnType.INTEGER},
                    { Text_type_col, DefinedColumnType.STRING},
                    { Keyword_type_col, DefinedColumnType.STRING},
                    { Date_type_col, DefinedColumnType.STRING},
                    { Geo_type_col, DefinedColumnType.STRING}
                };

                TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema, definedColumnSchema);

                CapacityUnit reservedThroughput = new CapacityUnit(0, 0);

                TableOptions tableOptions = new TableOptions();
                // 若需要支持多元索引TTL，需禁用数据表的UpdateRow功能。
                tableOptions.AllowUpdate = true;

                CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
                request.TableOptions = tableOptions;
                otsClient.CreateTable(request);

                Console.WriteLine("\n Table is created: " + TableName);
            }

            // 向数据表中写入数据
            {
                Console.WriteLine("\n Start put row...");
                List<string> colList = new List<string>() {
                    "TableStore SearchIndex Sample",
                    "TableStore",
                    "SearchIndex",
                    "Sample",
                    "SearchIndex Sample",
                    "TableStore Sample",
                    "TableStore SearchIndex"
                };

                for (int i = 0; i < 7; i++)
                {
                    PrimaryKey primaryKey = new PrimaryKey{
                    { Pk0, new ColumnValue(i) },
                    { Pk1, new ColumnValue("pk1value") }
                };
                    AttributeColumns attribute = new AttributeColumns{
                    { Long_type_col, new ColumnValue(i) },
                    { Text_type_col, new ColumnValue(colList[i]) },
                    { Keyword_type_col, new ColumnValue(colList[i]) },
                    { Date_type_col, new ColumnValue(DateTime.Now.ToString())},
                    { Geo_type_col, new ColumnValue(string.Format("{0},{1}", i , i + 1))}
                };
                    PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

                    otsClient.PutRow(request);
                }
                Console.WriteLine("\n Put row succeed.");
            }

            // 执行SQL
            {
                Console.WriteLine("\n SQLQuery start...");

                // 创建映射表
                string sqlQuery = @"CREATE TABLE `SearchIndexSampleTable` (
                        `pk0` BIGINT(20),
                        `pk1` VARCHAR(1024),
                        `Long_type_col` BIGINT(20),
                        `Text_type_col` MEDIUMTEXT,
                        `Keyword_type_col` MEDIUMTEXT,
                        `Data_type_col` MEDIUMTEXT,
                        `Geo_type_col` MEDIUMTEXT,
                        `Virtual_col_Text` MEDIUMTEXT,
                        PRIMARY KEY(`pk0`,`pk1`)
                    ); ";

                SQLQueryRequest sqlQueryRequest = new SQLQueryRequest(sqlQuery);
                SQLQueryResponse response = otsClient.SQLQuery(sqlQueryRequest);

                // 查询表的表述信息
                sqlQuery = "DESCRIBE `SearchIndexSampleTable`;";
                sqlQueryRequest = new SQLQueryRequest(sqlQuery);
                response = otsClient.SQLQuery(sqlQueryRequest);

                Console.WriteLine("Table Meta: {0}", JsonConvert.SerializeObject(response.GetSQLResultSet().GetSQLTableMeta()));

                // 查询表中的数据
                sqlQuery = "select * from " + TableName;

                sqlQueryRequest = new SQLQueryRequest(sqlQuery);
                response = otsClient.SQLQuery(sqlQueryRequest);

                ISQLResultSet selectResultSet = response.GetSQLResultSet();
                while (selectResultSet.HasNext())
                {
                    ISQLRow row = selectResultSet.Next();

                    Console.WriteLine(row.GetLong(Pk0) + " , " + row.GetString(Pk1) + " , " + row.GetLong(Long_type_col) + " , " +
                        row.GetString(Text_type_col) + " , " + row.GetString(Keyword_type_col) + " , " + row.GetString(Date_type_col) + " , " +
                        row.GetString(Geo_type_col) + " , " + row.GetString(Virtual_col_Text));
                }

                // 删除表的映射关系
                sqlQuery = "drop mapping table SearchIndexSampleTable";
                sqlQueryRequest = new SQLQueryRequest(sqlQuery);
                response = otsClient.SQLQuery(sqlQueryRequest);

                Console.WriteLine("\n SQLQuery finish!");
            }

            // 删除数据表
            {
                Console.WriteLine("\n Start delete table...");
                DeleteTableRequest request = new DeleteTableRequest(TableName);
                otsClient.DeleteTable(request);
                Console.WriteLine("\n Delete table finished!");
            }

            Console.WriteLine("\n SQL Sample finished!");
        }
    }
}
