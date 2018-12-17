using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;

namespace Aliyun.OTS.Samples.Samples
{
    public class SearchIndexPageSample
    {
        private static readonly string TableName = "SearchIndexPageSampleTable";
        private static readonly string IndexName = "SearchIndexPageSampleTableIndex";
        private static readonly string Pk0 = "pk0";
        private static readonly string Pk1 = "pk1";
        private static readonly string Long_type_col = "Long_type_col";
        private static readonly string Text_type_col = "Text_type_col";
        private static readonly string Keyword_type_col = "Keyword_type_col";

        static void Main(string[] args)
        {
            OTSClient otsClient = Config.GetClient();
            //DeleteSearchIndex(otsClient);
            //DeleteTable(otsClient);
            //CreateTable(otsClient);
            //CreateSearchIndex(otsClient);

            ////Wait searchIndex load success
            //Console.WriteLine("wait searchIndex load success");
            //Thread.Sleep(3 * 1000);

            //ListSearchIndex(otsClient);
            //DescribeSearchIndex(otsClient);
            //PutRow(otsClient);

            ////Wait searchIndex load success
            //WaiteAllDataSyncSuccess(otsClient, 7);

            ReadMoreRowsWithToken(otsClient);

            Console.ReadLine();
        }


        public static void CreateTable(OTSClient otsClient)
        {
            // 创建表
            Console.WriteLine("\n Start create table...");
            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
                {
                    { Pk0, ColumnValueType.Integer },
                    { Pk1, ColumnValueType.String }
                };
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

            Console.WriteLine("Table is created: " + TableName);
        }

        public static void DeleteTable(OTSClient otsClient)
        {
            DeleteTableRequest request = new DeleteTableRequest(TableName);
            otsClient.DeleteTable(request);
        }

        public static void ListSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start list searchindex...");

            ListSearchIndexRequest request = new ListSearchIndexRequest(TableName);
            ListSearchIndexResponse response = otsClient.ListSearchIndex(request);
            foreach (var index in response.IndexInfos)
            {
                Console.WriteLine("indexname:" + index.IndexName);
                Console.WriteLine("tablename:" + index.TableName);
            }
        }

        public static void CreateSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Create searchindex...");

            CreateSearchIndexRequest request = new CreateSearchIndexRequest(TableName, IndexName);
            List<FieldSchema> FieldSchemas = new List<FieldSchema>() {
                new FieldSchema(Keyword_type_col,FieldType.KEYWORD){index=true,EnableSortAndAgg=true},
                new FieldSchema(Long_type_col,FieldType.LONG){ index=true,EnableSortAndAgg=true},
                new FieldSchema(Text_type_col,FieldType.TEXT){ index=true}
            };
            request.IndexSchame = new IndexSchema()
            {
                FieldSchemas = FieldSchemas
            };

            CreateSearchIndexResponse response = otsClient.CreateSearchIndex(request);

            Console.WriteLine("Searchindex is created: " + IndexName);
        }

        public static void DescribeSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Describe searchindex...");

            DescribeSearchIndexRequest request = new DescribeSearchIndexRequest(TableName, IndexName);

            DescribeSearchIndexResponse response = otsClient.DescribeSearchIndex(request);
            string serializedObjectString = JsonConvert.SerializeObject(response);
            Console.WriteLine(serializedObjectString);

        }

        public static void DeleteSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Delete searchindex...");

            DeleteSearchIndexRequest request = new DeleteSearchIndexRequest(TableName, IndexName);
            DeleteSearchIndexResponse response = otsClient.DeleteSearchIndex(request);

            Console.WriteLine("Searchindex is deleted:" + IndexName);

        }

        public static void PutRow(OTSClient otsClient)
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
            for (int i = 0; i < 500; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey{
                    { Pk0, new ColumnValue(i) },
                    { Pk1, new ColumnValue("pk1value") }
                };
                var colId = i % 7;
                AttributeColumns attribute = new AttributeColumns{
                    { Long_type_col, new ColumnValue(i) },
                    { Text_type_col, new ColumnValue(colList[colId]) },
                    { Keyword_type_col, new ColumnValue(colList[colId]) }
                };
                PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

                otsClient.PutRow(request);
            }
            Console.WriteLine("Put row succeed.");
        }

        public static void WaiteAllDataSyncSuccess(OTSClient otsClient, int expectTotalCount)
        {
            Console.WriteLine("wait all rows sync success");
            int timeoutSeconds = 3 * 60;
            var beginTime = DateTime.Now;
            while (true)
            {
                var searchQuery = new SearchQuery();
                searchQuery.Query = new MatchAllQuery();
                var request = new SearchRequest(TableName, IndexName, searchQuery);

                var response = otsClient.Search(request);
                if (response.TotalCount == expectTotalCount)
                {
                    break;
                }
                else if ((DateTime.Now - beginTime).Seconds > timeoutSeconds)
                {
                    throw new Exception("searchIndex sync data timeout");
                }
                Thread.Sleep(1000);
            }
        }


        /// <summary>
        /// 查询所有行，返回行数
        /// </summary>
        /// <param name="otsClient"></param>
        public static SearchResponse ReadMoreRowsWithToken(OTSClient otsClient)
        {
            Console.WriteLine("\n Start matchAll query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchAllQuery();
            
            var request = new SearchRequest(TableName, IndexName, searchQuery);

            var response = otsClient.Search(request);
            var rows = response.Rows;
            while (response.NextToken != null)
            {
                request.SearchQuery.Token = response.NextToken;
                response = otsClient.Search(request);
                rows.AddRange(response.Rows);
            }

            return response;
        }

    }
}
