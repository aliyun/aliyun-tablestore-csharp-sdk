using System;
using System.Collections.Generic;
using System.Threading;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;

namespace Aliyun.OTS.Samples.Samples
{
    public class SearchIndexSample
    {
        private static readonly string TableName = "SearchIndexSampleTable";
        private static readonly string IndexName = "SearchIndexSampleTableIndex2";
        private static readonly string Pk0 = "pk0";
        private static readonly string Pk1 = "pk1";
        private static readonly string Long_type_col = "Long_type_col";
        private static readonly string Text_type_col = "Text_type_col";
        private static readonly string Keyword_type_col = "Keyword_type_col";

        //static void Main(string[] args)
        //{
        //    OTSClient otsClient = Config.GetClient();
        //    //DeleteSearchIndex(otsClient);
        //    //DeleteTable(otsClient);

        //    //创建一张TableStore表
        //    CreateTable(otsClient);
        //    //在TableStore表上创建一个索引表
        //    CreateSearchIndex(otsClient);

        //    //Wait searchIndex load success
        //    Console.WriteLine("wait searchIndex load success");
        //    Thread.Sleep(3 * 1000);

        //    ListSearchIndex(otsClient);
        //    CreateSearchIndexWithIndexSort(otsClient);
        //    DescribeSearchIndex(otsClient);
        //    PutRow(otsClient);

        //    //等待索引数据同步成功
        //    WaiteAllDataSyncSuccess(otsClient, 7);

        //    //MatchAll Query
        //    MatchAllQuery(otsClient);

        //    //MatchQuery
        //    MatchQuery(otsClient);

        //    //MatchPhraseQuery
        //    MatchPhraseQuery(otsClient);

        //    //RangeQuery
        //    RangeQuery(otsClient);

        //    //PrefixQuery
        //    PrefixQuery(otsClient);

        //    //TermQuery
        //    TermQuery(otsClient);

        //    //WildcardQuery
        //    WildcardQuery(otsClient);

        //    //BoolQuery
        //    BoolQuery(otsClient);

        //    Console.ReadLine();
        //}

        public static void CreateTable(OTSClient otsClient)
        {
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

        /// <summary>
        /// 列出多元索引名称
        /// </summary>
        /// <param name="otsClient"></param>
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

        /// <summary>
        /// 创建一个多元索引，包含Keyword_type_col、Long_type_col、Text_type_col三个属性列，类型分别设置为不分词字符串(KEYWORD)，整型(LONG)，分词字符串(TEXT)
        /// </summary>
        /// <param name="otsClient"></param>
        public static void CreateSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Create searchindex...");

            //指定表名和索引名
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(TableName, IndexName);
            List<FieldSchema> FieldSchemas = new List<FieldSchema>() {
                new FieldSchema(Keyword_type_col,FieldType.KEYWORD){ //设置字段名和字段类型
                    index =true, //开启索引
                    EnableSortAndAgg =true //开启排序和统计功能
                },
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

        /// <summary>
        /// 创建一个多元索引，包含Keyword_type_col、Long_type_col、Text_type_col三个属性列，类型分别设置为不分词字符串(KEYWORD)，整型(LONG)，分词字符串(TEXT)
        /// </summary>
        /// <param name="otsClient"></param>
        public static void CreateSearchIndexWithIndexSort(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Create searchindex with indexSort...");

            //指定表名和索引名
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(TableName, IndexName);
            List<FieldSchema> FieldSchemas = new List<FieldSchema>() {
                new FieldSchema(Keyword_type_col,FieldType.KEYWORD){ //设置字段名和字段类型
                    index =true, //开启索引
                    EnableSortAndAgg =true //开启排序和统计功能
                },
                new FieldSchema(Long_type_col,FieldType.LONG){ index=true,EnableSortAndAgg=true},
                new FieldSchema(Text_type_col,FieldType.TEXT){ index=true}
            };
            request.IndexSchame = new IndexSchema()
            {
                FieldSchemas = FieldSchemas,
                IndexSort = new Sort(new List<ISorter>()
                    {
                        new FieldSort(Long_type_col,SortOrder.ASC)
                    })
            };

            CreateSearchIndexResponse response = otsClient.CreateSearchIndex(request);

            Console.WriteLine("Searchindex is created: " + IndexName);
        }

        /// <summary>
        /// 查询多元索引的描述信息
        /// </summary>
        /// <param name="otsClient"></param>
        public static void DescribeSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Describe searchindex...");

            //设置表名和索引名
            DescribeSearchIndexRequest request = new DescribeSearchIndexRequest(TableName, IndexName);

            DescribeSearchIndexResponse response = otsClient.DescribeSearchIndex(request);
            string serializedObjectString = JsonConvert.SerializeObject(response);
            Console.WriteLine(serializedObjectString);
        }

        /// <summary>
        /// 删除多元索引
        /// </summary>
        /// <param name="otsClient"></param>
        public static void DeleteSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n Start Delete searchindex...");

            //设置表名和索引名
            DeleteSearchIndexRequest request = new DeleteSearchIndexRequest(TableName, IndexName);
            DeleteSearchIndexResponse response = otsClient.DeleteSearchIndex(request);

            Console.WriteLine("Searchindex is deleted:" + IndexName);
        }

        /// <summary>
        /// 通过PutRow 接口写入一些数据到TableStore，数据将自动同步到索引表
        /// </summary>
        /// <param name="otsClient"></param>
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
            for (int i = 0; i < 7; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey{
                    { Pk0, new ColumnValue(i) },
                    { Pk1, new ColumnValue("pk1value") }
                };
                AttributeColumns attribute = new AttributeColumns{
                    { Long_type_col, new ColumnValue(i) },
                    { Text_type_col, new ColumnValue(colList[i]) },
                    { Keyword_type_col, new ColumnValue(colList[i]) }
                };
                PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

                otsClient.PutRow(request);
            }
            Console.WriteLine("Put row succeed.");
        }

        /// <summary>
        /// 等待数据同步到索引表完成
        /// </summary>
        /// <param name="otsClient"></param>
        /// <param name="expectTotalCount"></param>
        public static void WaiteAllDataSyncSuccess(OTSClient otsClient, int expectTotalCount)
        {
            Console.WriteLine("wait all rows sync success");
            int timeoutSeconds = 3 * 60;
            var beginTime = DateTime.Now;
            while (true)
            {
                var searchQuery = new SearchQuery();
                searchQuery.GetTotalCount = true;
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
            PrimaryKey primaryKeyRead = row.PrimaryKey;
            AttributeColumns attributesRead = row.AttributeColumns;

            Console.WriteLine("Primary key: ");
            foreach (KeyValuePair<string, ColumnValue> entry in primaryKeyRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }

            Console.WriteLine("Attributes: ");
            foreach (KeyValuePair<string, ColumnValue> entry in attributesRead)
            {
                Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
            }
        }

        /// <summary>
        /// 查询所有行，返回行数
        /// </summary>
        /// <param name="otsClient"></param>
        public static void MatchAllQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start matchAll query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchAllQuery();
            searchQuery.GetTotalCount = true; // 需要设置GetTotalCount = true 才会返回满足条件的数据总行数
            /*
            * MatchAllQuery 结果中的Totalcount可以表示数据的总行数（数据量很大时为估算值）
            * 如果只是为了查询TotalHit，可以设置limit=0，即不返回任意一行数据。
            */
            searchQuery.Limit = 0;
            var request = new SearchRequest(TableName, IndexName, searchQuery);

            var response = otsClient.Search(request);

            Console.WriteLine("IsAllSuccess:" + response.IsAllSuccess);
            Console.WriteLine("Total Count:" + response.TotalCount);
        }


        /// <summary>
        /// 模糊匹配和短语或邻近查询，返回指定列
        /// </summary>
        /// <param name="otsClient"></param>
        public static void MatchQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start match query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchQuery(Text_type_col, "SearchIndex");
            searchQuery.GetTotalCount = true;
            var request = new SearchRequest(TableName, IndexName, searchQuery);
            searchQuery.Sort = new Sort(new List<ISorter>() { new ScoreSort() });
            request.ColumnsToGet = new ColumnsToGet()
            {
                Columns = new List<string>() { Long_type_col, Text_type_col, Keyword_type_col }
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        /// 类似MatchQuery（MatchQuery 仅匹配某个词即可），但是 MatchPhraseQuery会匹配所有的短语。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void MatchPhraseQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start MatchPhrase query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchPhraseQuery(Text_type_col, "TableStore SearchIndex");
            searchQuery.GetTotalCount = true;
            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                Columns = new List<string>() { Long_type_col, Text_type_col, Keyword_type_col }
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        /// 范围查询。通过设置一个范围（from，to），查询该范围内的所有数据。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void RangeQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start range query...");

            var searchQuery = new SearchQuery();
            searchQuery.GetTotalCount = true;
            var rangeQuery = new RangeQuery(Long_type_col, new ColumnValue(0), new ColumnValue(6));
            //包括下边界（大于等于0）
            rangeQuery.IncludeLower = true;
            searchQuery.Query = rangeQuery;
            var request = new SearchRequest(TableName, IndexName, searchQuery);
            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }


        /// <summary>
        /// 前缀查询。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void PrefixQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start prefix query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new PrefixQuery(Keyword_type_col, "Search");
            searchQuery.GetTotalCount = true;
            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        /// 精确的term查询
        /// </summary>
        /// <param name="otsClient"></param>
        public static void TermQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start term query...");

            var searchQuery = new SearchQuery();
            searchQuery.GetTotalCount = true;
            searchQuery.Query = new TermQuery(Keyword_type_col, new ColumnValue("SearchIndex"));

            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        /// 精确的terms查询，查询Keyword_type_col这一列精确匹配"TableStore"或者"SearchIndex"的数据
        /// TermsQuery可以使用多个Term同时查询
        /// </summary>
        /// <param name="otsClient"></param>
        public static void TermsQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start terms query...");

            var searchQuery = new SearchQuery();
            searchQuery.GetTotalCount = true;
            var query = new TermsQuery();
            query.FieldName = Keyword_type_col;
            query.Terms = new List<ColumnValue>() { new ColumnValue("TableStore"), new ColumnValue("SearchIndex") };

            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }



        /// <summary>
        ///通配符查询。支持 *（ 任意0或多个）和 ？（任意1个字符）。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void WildcardQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start wildcard query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new WildcardQuery(Keyword_type_col, "*Search*");
            searchQuery.GetTotalCount = true;
            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        ///联合查询（复杂查询条件下用的最多的一个查询）。Bool查询由一个或者多个子句组成，每个子句都有特定的类型。
        ///must: 文档必须完全匹配条件
        ///should: should下面会带一个以上的条件，至少满足一个条件，这个文档就符合should
        ///must_not: 文档必须不匹配条件
        ///MinimumShouldMatch: should查询的条件至少满足几个
        /// </summary>
        /// <param name="otsClient"></param>
        public static void BoolQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start bool query...");

            var searchQuery = new SearchQuery();
            searchQuery.GetTotalCount = true;
            var boolQuery = new BoolQuery();
            var shouldQuerys = new List<IQuery>();
            shouldQuerys.Add(new TermQuery(Keyword_type_col, new ColumnValue("SearchIndex")));
            shouldQuerys.Add(new TermQuery(Keyword_type_col, new ColumnValue("TableStore")));
            boolQuery.ShouldQueries = shouldQuerys;
            boolQuery.MinimumShouldMatch = 1;

            searchQuery.Query = boolQuery;

            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary>
        /// 有一个类型为NESTED的列，子文档包含nested_1和nested_2两列，现在查询col1_nested.nested_1为"tablestore"的数据
        /// </summary>
        /// <param name="otsClient"></param>
        public static void NestedQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start bool query...");

            var searchQuery = new SearchQuery();
            searchQuery.GetTotalCount = true;
            var nestedQuery = new NestedQuery();
            nestedQuery.Path = "col1_nested"; //设置nested字段路径
            TermQuery termQuery = new TermQuery("col1_nested.nested_1",new ColumnValue("tablestore"));//构造NestedQuery的子查询
            nestedQuery.Query = termQuery;
            nestedQuery.ScoreMode = ScoreMode.None;

            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.ColumnsToGet = new ColumnsToGet()
            {
                ReturnAll = true
            };

            var response = otsClient.Search(request);

            Console.WriteLine("Total Count:" + response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }
    }
}
