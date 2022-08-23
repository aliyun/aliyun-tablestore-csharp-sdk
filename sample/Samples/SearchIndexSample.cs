using System;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.Analysis;
using Aliyun.OTS.DataModel.Search.GroupBy;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;


namespace Aliyun.OTS.Samples.Samples
{
    public class SearchIndexSample
    {
        private static readonly string TableName = "SearchIndexSampleTable";
        private static readonly string IndexName = "SearchIndexSampleTableIndex2";
        private static readonly string IndexNamewithSort = "SearchIndexSampleTableIndexwithSort";
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
            Console.WriteLine("Start SeaIndexSample...");
            OTSClient otsClient = Config.GetClient();

            //创建一张TableStore表
            CreateTable(otsClient);
            //在TableStore表上创建一个索引表
            CreateSearchIndex(otsClient);

            //Wait searchIndex load success
            Console.WriteLine("wait searchIndex load success");
            Thread.Sleep(3 * 1000);

            ListSearchIndex(otsClient);

            CreateSearchIndexWithIndexSort(otsClient);
            DescribeSearchIndex(otsClient);
            PutRow(otsClient);

            //等待索引数据同步成功
            WaiteAllDataSyncSuccess(otsClient, 7);

            //MatchAll Query
            MatchAllQuery(otsClient);

            //MatchQuery
            MatchQuery(otsClient);

            //MatchPhraseQuery
            MatchPhraseQuery(otsClient);

            //RangeQuery
            RangeQuery(otsClient);

            //PrefixQuery
            PrefixQuery(otsClient);

            //TermQuery
            TermQuery(otsClient);

            //WildcardQuery
            WildcardQuery(otsClient);

            //BoolQuery
            BoolQuery(otsClient);

            //ParallelScan
            ParallelScan(otsClient);

            //UpdateSearchIndex
            UpdateSearchIndex(otsClient);

            //GroupBy
            GroupBy(otsClient);

            // Aggregation
            Aggregation(otsClient);

            // Delete SearchIndex and Table
            DeleteSearchIndex(otsClient);
            DeleteTable(otsClient);

            Console.WriteLine("SearchIndexSample Finish！");
            Console.ReadLine();
        }

        public static void CreateTable(OTSClient otsClient)
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
            tableOptions.AllowUpdate = false;

            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            request.TableOptions = tableOptions;
            otsClient.CreateTable(request);

            Console.WriteLine("Table is created: " + TableName);
        }

        public static void DeleteTable(OTSClient otsClient)
        {
            try
            {
                DeleteTableRequest request = new DeleteTableRequest(TableName);
                otsClient.DeleteTable(request);
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("Delete table failed with error: {0}", e.ErrorMessage);
            }

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
            request.SetTimeToLive(7 * 24 * 60 * 60);

            List<FieldSchema> FieldSchemas = new List<FieldSchema>() {
                new FieldSchema(Keyword_type_col,FieldType.KEYWORD){ //设置字段名和字段类型
                    index =true, //开启索引
                    EnableSortAndAgg =true //开启排序和统计功能
                },
                new FieldSchema(Pk1, FieldType.KEYWORD) { index=true, EnableSortAndAgg=true },
                new FieldSchema(Long_type_col,FieldType.LONG){ index=true,EnableSortAndAgg=true},
                new FieldSchema(Text_type_col,FieldType.TEXT){ index=true },
                // 构建虚拟列
                new FieldSchema(Virtual_col_Text, FieldType.TEXT){
                    index=true,
                    SourceFieldNames= new List<string>(){ Text_type_col },
                    IsVirtualField = true,
                    Analyzer = Analyzer.Split,
                    AnalyzerParameter = new SplitAnalyzerParameter(){
                        Delimiter=" "
                    }
                },
                // 构建日期列
                new FieldSchema(Date_type_col, FieldType.DATE) {
                    index = true,
                    DateFormats = new List<string>(){
                        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                        "yyyy-MM-dd'T'HH:mm:ss.SSS",
                        "yyyy/MM/dd'T'HH:mm:ss"
                    }
                },

                // 构建地理位置列
                new FieldSchema(Geo_type_col, FieldType.GEO_POINT)
                {
                    index = true,
                    EnableSortAndAgg = true
                }
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
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(TableName, IndexNamewithSort);
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

            Console.WriteLine("Searchindex is created: " + IndexNamewithSort);
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

            // 删除索引表
            try
            {
                foreach (var indexInfo in otsClient.ListSearchIndex(new ListSearchIndexRequest(TableName)).IndexInfos)
                {

                    DeleteSearchIndexRequest deleteSearchIndexRequest = new DeleteSearchIndexRequest(TableName, indexInfo.IndexName);
                    otsClient.DeleteSearchIndex(deleteSearchIndexRequest);

                    Thread.Sleep(1000);

                }
            }

            catch (OTSException e)
            {
                Console.WriteLine("Delete Index or Table failed with error: {0}", e.Message);
            }

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
                    { Keyword_type_col, new ColumnValue(colList[i]) },
                    { Date_type_col, new ColumnValue(DateTime.Now.ToString())},
                    { Geo_type_col, new ColumnValue(string.Format("{0},{1}", i , i + 1))}
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

            Console.WriteLine("\n matchAll query finished!");
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

            Console.WriteLine("\n match query finished!");
        }

        /// <summary>
        /// 类似MatchQuery（MatchQuery 仅匹配某个词即可），但是 MatchPhraseQuery会匹配所有的短语。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void MatchPhraseQuery(OTSClient otsClient)
        {
            Console.WriteLine("\n Start MatchPhrase query...");

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchPhraseQuery(Text_type_col, "SearchIndex");
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

            Console.WriteLine("\n MatchPhrase query finished!");
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

            Console.WriteLine("\n range query finished!");
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

            Console.WriteLine("\n prefix query finished!");
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

            Console.WriteLine("\n Term query finished!");
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
            var query = new TermsQuery(
                    Keyword_type_col,
                    new List<ColumnValue>() { new ColumnValue("TableStore"), new ColumnValue("SearchIndex") }
                );

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

            Console.WriteLine("\n Terms query finished!");
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
            TermQuery termQuery = new TermQuery("col1_nested.nested_1", new ColumnValue("tablestore"));//构造NestedQuery的子查询
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

        /// <summary>
        /// 当使用场景中不关心整个结果集的顺序时，可以使用并发导出数据功能以更快的速度将命中的数据全部返回。
        /// </summary>
        /// <param name="otsClient"></param>
        public static void ParallelScan(OTSClient otsClient)
        {
            Console.WriteLine("\n Start ParallelScan...");

            SearchIndexSplitsOptions indexOptions = new SearchIndexSplitsOptions(IndexName);
            ComputeSplitsRequest computeSplitsRequest = new ComputeSplitsRequest(TableName, indexOptions);

            ComputeSplitsResponse computeSplitResponse = otsClient.ComputeSplits(computeSplitsRequest);

            ScanQuery scanQuery = new ScanQuery();
            scanQuery.Limit = 10;
            scanQuery.MaxParallel = computeSplitResponse.SplitsSize;
            scanQuery.Query = new MatchAllQuery();

            ParallelScanRequest request = new ParallelScanRequest(TableName, IndexName);
            request.ScanQuery = scanQuery;
            request.SessionId = computeSplitResponse.SessionId;
            request.ColumnToGet = new ColumnsToGet()
            {
                ReturnAllFromIndex = true,
            };

            ParallelScanResponse response = otsClient.ParallelScan(request);
            while (response.NextToken != null)
            {
                Console.WriteLine(JsonConvert.SerializeObject(response.Rows));

                request.ScanQuery.Token = response.NextToken;
                response = otsClient.ParallelScan(request);
            }

            Console.WriteLine("\n ParallelScan finshed!");
        }

        /// <summary>
        /// 更新SearchIndex的TTL
        /// </summary>
        /// <param name="otsClient"></param>
        public static void UpdateSearchIndex(OTSClient otsClient)
        {
            Console.WriteLine("\n UpdateSearchIndex start...");

            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest(TableName, IndexName);
            request.SetTimeToLive(14 * 24 * 60 * 60);      // two weeks

            otsClient.UpdateSearchIndex(request);

            Console.WriteLine("\n UpdateSeatchIndex finish!");
        }

        public static void GroupBy(OTSClient otsClient)
        {
            Console.WriteLine("\n GroupBy start...");

            GroupByField groupByField = new GroupByField();
            groupByField.GroupByName = "groupByField";
            groupByField.FieldName = Long_type_col;
            groupByField.GroupBySorters = new List<GroupBySorter>(){
                new GroupBySorter()
                {
                    GroupKeySort = new GroupKeySort() { Order = SortOrder.DESC }
                }
            };

            GroupByFilter groupByFilter = new GroupByFilter();
            groupByFilter.GroupByName = "groupByFilter";
            groupByFilter.Filters = new List<IQuery>()
            {
                new MatchPhraseQuery(Virtual_col_Text, "Tablestore")
            };

            GroupByGeoDistance groupByGeoDistance = new GroupByGeoDistance();
            groupByGeoDistance.FieldName = Geo_type_col;
            groupByGeoDistance.GroupByName = "groupByGeoDistance";
            groupByGeoDistance.Origin = new GeoPoint(0, 0);
            groupByGeoDistance.Ranges = new List<DataModel.Search.GroupBy.Range>()
            {
                new DataModel.Search.GroupBy.Range(double.MinValue, 100.0),
                new DataModel.Search.GroupBy.Range(100.0, 1000.0),
                new DataModel.Search.GroupBy.Range(1000.0, double.MaxValue)
            };

            GroupByHistogram groupByHistogram = new GroupByHistogram();
            groupByHistogram.GroupByName = "groupByHistogram";
            groupByHistogram.FieldName = Long_type_col;
            groupByHistogram.Interval = new ColumnValue(2);
            groupByHistogram.Missing = new ColumnValue(2);
            groupByHistogram.FieldRange = new FieldRange(new ColumnValue(0), new ColumnValue(1000));

            GroupByRange groupByRange = new GroupByRange();
            groupByRange.GroupByName = "groupByRange";
            groupByRange.FieldName = Long_type_col;
            groupByRange.Ranges = new List<DataModel.Search.GroupBy.Range>()
            {
                new DataModel.Search.GroupBy.Range(0 , 5),
                new DataModel.Search.GroupBy.Range(5,10),
                new DataModel.Search.GroupBy.Range(10, double.MaxValue)
            };

            GroupByResults results = GroupByRunner(otsClient, groupByField, groupByFilter, groupByGeoDistance, groupByHistogram, groupByRange);

            if (results != null)
            {
                Console.WriteLine("groupByField result: {0}", JsonConvert.SerializeObject(results.GetAsGroupByFieldResult("groupByField").GroupByFieldResultItems));
                Console.WriteLine("groupByFilter result: {0}", JsonConvert.SerializeObject(results.GetAsGroupByFilterResult("groupByFilter").GroupByFilterResultItems));
                Console.WriteLine("groupByGeoDistance result: {0}", JsonConvert.SerializeObject(results.GetAsGroupByGeoDistanceResult("groupByGeoDistance").GroupByGeoDistanceResultItems));
                Console.WriteLine("groupByHistogram result: {0}", JsonConvert.SerializeObject(results.GetAsGroupByHistogramResult("groupByHistogram").GroupByHistogramResultItems));
                Console.WriteLine("groupByRange result: {0}", JsonConvert.SerializeObject(results.GetAsGroupByRangeResult("groupByRange").GroupByRangeResultItems));
            }

            Console.WriteLine("\n GroupBy finish!");
        }

        public static void Aggregation(OTSClient otsClient)
        {
            Console.WriteLine("\n Aggregation start...");

            MaxAggregation maxAggregation = new MaxAggregation();
            maxAggregation.AggName = "maxAgg";
            maxAggregation.FieldName = Long_type_col;
            maxAggregation.Missing = new ColumnValue(10);

            MinAggregation minAggregation = new MinAggregation();
            minAggregation.AggName = "minAgg";
            minAggregation.FieldName = Long_type_col;
            minAggregation.Missing = new ColumnValue(10);

            AvgAggregation avgAggregation = new AvgAggregation();
            avgAggregation.AggName = "avgAgg";
            avgAggregation.FieldName = Long_type_col;
            avgAggregation.Missing = new ColumnValue(10);

            CountAggregation countAggregation = new CountAggregation();
            countAggregation.AggName = "countAgg";
            countAggregation.FieldName = Pk1;

            DistinctCountAggregation distinctCountAggregation = new DistinctCountAggregation();
            distinctCountAggregation.AggName = "distinctCountAgg";
            distinctCountAggregation.FieldName = Pk1;
            distinctCountAggregation.Missing = new ColumnValue("pk1Value");

            PercentilesAggregation percentilesAggregation = new PercentilesAggregation();
            percentilesAggregation.AggName = "percentilesAgg";
            percentilesAggregation.FieldName = Long_type_col;
            percentilesAggregation.Missing = new ColumnValue(10);
            percentilesAggregation.Percentiles = new List<double>()
            {
                60,
                90,
                99
            };

            SumAggregation sumAggregation = new SumAggregation();
            sumAggregation.AggName = "sumAgg";
            sumAggregation.FieldName = Long_type_col;
            sumAggregation.Missing = new ColumnValue(10);

            TopRowsAggregation topRowsAggregation = new TopRowsAggregation();
            topRowsAggregation.AggName = "topRowsAgg";
            topRowsAggregation.Limit = 5;
            topRowsAggregation.Sort = new Sort(new List<ISorter>() { new FieldSort(Long_type_col, SortOrder.DESC) });

            AggregationResults results = AggregationRunner(otsClient, maxAggregation, minAggregation, avgAggregation, countAggregation,
                distinctCountAggregation, percentilesAggregation, sumAggregation, topRowsAggregation);

            if (results != null)
            {
                Console.WriteLine("maxAgg result: {0}", results.GetAsMaxAggregationResult("maxAgg").Value);
                Console.WriteLine("minAgg result: {0}", results.GetAsMinAggregationResult("minAgg").Value);
                Console.WriteLine("avgAgg result: {0}", results.GetAsAvgAggregationResult("avgAgg").Value);
                Console.WriteLine("countAgg result: {0}", results.GetAsCountAggregationResult("countAgg").Value);
                Console.WriteLine("distinctCountAgg result: {0}", results.GetAsDistinctCountAggregationResult("distinctCountAgg").Value);
                Console.WriteLine("percentilesAgg result: {0}", JsonConvert.SerializeObject(results.GetAsPercentilesAggregationResult("percentilesAgg").Value));
                Console.WriteLine("sumAgg result: {0}", results.GetAsSumAggregationResult("sumAgg").Value);
                Console.WriteLine("topRowsAgg result: {0}", JsonConvert.SerializeObject(results.GetAsTopRowsAggregationResult("topRowsAgg").Rows));
            }

            Console.WriteLine("\n Aggregation finished!");
        }

        public static AggregationResults AggregationRunner(OTSClient otsClient, params IAggregation[] aggregations)
        {
            if (aggregations.Length == 0)
            {
                return null;
            }

            MatchAllQuery query = new MatchAllQuery();

            SearchQuery searchQuery = new SearchQuery
            {
                Query = query,
                AggregationList = new List<IAggregation>()
            };

            foreach (IAggregation agg in aggregations)
            {
                searchQuery.AggregationList.Add(agg);
            }

            SearchRequest searchRequest = new SearchRequest(TableName, IndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = otsClient.Search(searchRequest);

            return response.AggregationResults;
        }

        public static GroupByResults GroupByRunner(OTSClient otsClient, params IGroupBy[] groupBys)
        {
            if (groupBys.Length == 0)
            {
                return null;
            }

            MatchAllQuery query = new MatchAllQuery();

            SearchQuery searchQuery = new SearchQuery
            {
                Query = query,
                GroupByList = new List<IGroupBy>()
            };

            foreach (IGroupBy groupBy in groupBys)
            {
                searchQuery.GroupByList.Add(groupBy);
            }

            SearchRequest searchRequest = new SearchRequest(TableName, IndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = otsClient.Search(searchRequest);

            return response.GroupByResults;
        }
    }
}
