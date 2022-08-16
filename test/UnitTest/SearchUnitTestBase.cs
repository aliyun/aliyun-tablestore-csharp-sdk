using Aliyun.OTS.DataModel;
using System.Collections.Generic;
using NUnit.Framework;
using System.Threading;
using Aliyun.OTS.UnitTest;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.DataModel.Search;
using System;
using Aliyun.OTS.DataModel.Search.Analysis;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.Query;
using Newtonsoft.Json;
using Aliyun.OTS.DataModel.Search.GroupBy;
using System.Text;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    public class SearchUnitTestBase
    {
        public string TestEndPoint = Test.Config.Endpoint;
        public string TestAccessKeyID = Test.Config.AccessKeyId;
        public string TestAccessKeySecret = Test.Config.AccessKeySecret;
        public string TestInstanceName = Test.Config.InstanceName;
        public OTSClient OTSClient;

        // column name defined
        public static string Pk0_String = "pk0";
        public static string Pk1_Integer = "pk1";
        public static string Col0_String = "col0";
        public static string Col1_Integer = "col1";
        public static string Col2_Double = "col2";
        public static string Col3_Boolean = "col3";
        public static string Col4_Binary = "col4";

        // virtual column
        public static string Col0_Virtual1 = "col0_v1";
        public static string Col0_Virtual2 = "col0_v2";

        // Date column
        public static string Date_Column = "date_col";

        // GEO_Point
        public static string Geo_Column = "geo_col";

        // Predefined test data
        public static string TestTableName = "SearchTestBaseTable_1";
        public static string TestSearchIndexName = "SearchTestBaseIndex_1";
        public static string TestSearchVirtualIndexName = "SearchTestBaseIndexVirtual_1";
        public static PrimaryKeySchema PrimaryKeySchema;
        public static DefinedColumnSchema DefinedColumnSchema;
        public static PrimaryKey PrimaryKeyWith4Columns;
        public static AttributeColumns AttributeWith5Columns;
        public static List<PrimaryKey> PrimaryKeyList;
        public static List<AttributeColumns> AttributeColumnsList;

        // Search Column
        public static FieldSchema Col1IntegerSchema = new FieldSchema(Col1_Integer, FieldType.KEYWORD)
        {
            index = true,
            EnableSortAndAgg = true
        };

        public static FieldSchema Col0VirtualSchema = new FieldSchema(Col0_Virtual1, FieldType.TEXT)
        {
            index = true,
            Analyzer = Analyzer.Split,
            AnalyzerParameter = new SingleWordAnalyzerParameter(true, true),
            IsVirtualField = true,
            SourceFieldNames = new List<string> { Col0_String }
        };

        public static FieldSchema Col0Virtual2Schema = new FieldSchema(Col0_Virtual2, FieldType.TEXT)
        {
            index = true,
            Analyzer = Analyzer.SingleWord,
            AnalyzerParameter = new FuzzyAnalyzerParameter(1, 5),
            IsVirtualField = true,
            SourceFieldNames = new List<string> { Col0_String }
        };

        public static FieldSchema COl2Schema = new FieldSchema(Col2_Double, FieldType.DOUBLE)
        {
            index = true,
            EnableSortAndAgg = true
        };

        public static FieldSchema Pk0Schema = new FieldSchema(Pk0_String, FieldType.KEYWORD)
        {
            index = true,
            EnableSortAndAgg = true
        };

        public static FieldSchema Pk1Schema = new FieldSchema(Pk1_Integer, FieldType.LONG)
        {
            index = true,
            EnableSortAndAgg = true
        };

        public static FieldSchema DateSchema = new FieldSchema(Date_Column, FieldType.DATE)
        {
            index = true,
            DateFormats = new List<string>(){
                        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                        "yyyy-MM-dd'T'HH:mm:ss.SSS"
                    }
        };

        public static FieldSchema GeoSchema = new FieldSchema(Geo_Column, FieldType.GEO_POINT)
        {
            index = true,
            EnableSortAndAgg = true
        };

        public static int ExceptionRowCount = 15;
        public static int ExceptionTimeToLine = 7 * 24 * 60 * 60;

        [OneTimeSetUp]
        public void Setup()
        {
            Thread.Sleep(1000);

            OTSClientConfig clientConfig = new OTSClientConfig(TestEndPoint, TestAccessKeyID, TestAccessKeySecret, TestInstanceName);

            OTSClient = new OTSClient(clientConfig);
            clientConfig.OTSDebugLogHandler = LogToFileHandler.DefaultDebugLogHandler;
            clientConfig.OTSErrorLogHandler = LogToFileHandler.DefaultErrorLogHandler;

            OTSClientTestHelper.Reset();

            DeleteAllTable();

            Thread.Sleep(1000);

            // 创建数据表
            CreateTable();

            Thread.Sleep(1000);

            // 创建多元索引
            CreateSearchIndex();

            // 写入数据
            PutRows();

            // 等待数据同步
            WaiteAllDataSyncSuccess();
        }

        public void DeleteAllTable()
        {
            // 删除数据表
            try
            {
                ListSearchIndexRequest listSearchIndexRequest = new ListSearchIndexRequest(TestTableName);
                ListSearchIndexResponse listSearchIndexResponse = OTSClient.ListSearchIndex(listSearchIndexRequest);

                foreach (SearchIndexInfo indexInfo in listSearchIndexResponse.IndexInfos)
                {
                    DeleteSearchIndex(indexInfo.TableName, indexInfo.IndexName);
                }

                Thread.Sleep(1000);

                DeleteTable();
            }
            catch (OTSServerException e)
            {
                Console.WriteLine("Delete Index or Table failed! Error Message: {0}", e.ErrorMessage);
            }
        }

        public void DeleteSearchIndex(string tableName, string searchIndexName)
        {
            DeleteSearchIndexRequest deleteSearchIndexRequest = new DeleteSearchIndexRequest(tableName, searchIndexName);
            OTSClient.DeleteSearchIndex(deleteSearchIndexRequest);
        }

        public void DeleteTable(string tableName = null)
        {
            if (tableName == null)
            {
                tableName = TestTableName;
            }

            var otsClient = OTSClient;
            var deleteTableRequest = new DeleteTableRequest(tableName);
            otsClient.DeleteTable(deleteTableRequest);
        }

        public void CreateTable(string tableName = null)
        {
            if (tableName == null)
            {
                tableName = TestTableName;
            }

            PrimaryKeySchema = new PrimaryKeySchema
            {
                {Pk0_String , ColumnValueType.String},
                {Pk1_Integer , ColumnValueType.Integer},
            };

            DefinedColumnSchema = new DefinedColumnSchema(){
                { Col0_String ,  DefinedColumnType.STRING},
                { Col1_Integer , DefinedColumnType.INTEGER},
                { Col2_Double ,  DefinedColumnType.DOUBLE},
                { Col3_Boolean , DefinedColumnType.BOOLEAN},
                { Col4_Binary ,  DefinedColumnType.BINARY},
                { Date_Column,   DefinedColumnType.STRING},
                { Geo_Column,    DefinedColumnType.STRING}
            };

            TableMeta tableMeta = new TableMeta(tableName, PrimaryKeySchema, DefinedColumnSchema);

            CapacityUnit capacity = new CapacityUnit(0, 0);

            TableOptions tableOptions = new TableOptions();
            // 测试包含索引TTL功能，须禁止数据表的UpdateRow更新写入功能。
            // 数据表TTL是属性列级别，多元索引TTL是行级别，避免因UpdateRow造成的数据不一致现象。
            tableOptions.AllowUpdate = false;

            CreateTableRequest request = new CreateTableRequest(tableMeta, capacity);
            request.TableOptions = tableOptions;

            OTSClient.CreateTable(request);
        }

        public void CreateSearchIndex(string tableName = null, string searchIndexName = null)
        {
            if (tableName == null)
            {
                tableName = TestTableName;
            }

            if (searchIndexName == null)
            {
                searchIndexName = TestSearchIndexName;
            }

            List<FieldSchema> fieldSchemas = new List<FieldSchema> {
                Col1IntegerSchema,

                Col0VirtualSchema,

                Col0Virtual2Schema,

                COl2Schema,

                Pk0Schema,

                Pk1Schema,

                DateSchema,

                GeoSchema
            };

            CreateSearchIndexRequest request = new CreateSearchIndexRequest(tableName, searchIndexName);

            request.IndexSchame = new IndexSchema()
            {
                FieldSchemas = fieldSchemas,
                IndexSort = new OTS.DataModel.Search.Sort.Sort(
                    new List<OTS.DataModel.Search.Sort.ISorter>
                    {
                        new OTS.DataModel.Search.Sort.FieldSort(Pk1_Integer, OTS.DataModel.Search.Sort.SortOrder.ASC)
                    })
            };
            request.TimeToLive = ExceptionTimeToLine;

            OTSClient.CreateSearchIndex(request);
        }

        public void PutRows()
        {
            Condition condition = new Condition();
            condition.RowExistenceExpect = RowExistenceExpectation.IGNORE;

            List<string> colList = new List<string>() {
                "TableStore SearchIndex Sample",
                "TableStore",
                "SearchIndex",
                "Sample",
                "SearchIndex Sample",
                "TableStore Sample",
                "TableStore SearchIndex"
            };

            for (int i = 0; i < ExceptionRowCount; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey{
                    { Pk0_String, new ColumnValue(colList[i % 7]) },
                    { Pk1_Integer, new ColumnValue(i) }
                };
                AttributeColumns attribute = new AttributeColumns{
                    { Col0_String, new ColumnValue(colList[i % 7]) },
                    { Col1_Integer, new ColumnValue(i + 10) },
                    { Col2_Double, new ColumnValue((double)i) },
                    { Col3_Boolean, new ColumnValue((i % 2 == 0))},
                    { Col4_Binary, new ColumnValue(Encoding.UTF8.GetBytes(colList[i % 7]))},
                    { Date_Column, new ColumnValue(DateTime.Now.ToString())},
                    { Geo_Column, new ColumnValue(string.Format("{0},{1}", i , i + 1))}
                };
                PutRowRequest request = new PutRowRequest(TestTableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

                OTSClient.PutRow(request);
            }
        }

        public void WaiteAllDataSyncSuccess()
        {
            int timeoutSeconds = 3 * 60;
            var beginTime = DateTime.Now;
            while (true)
            {
                var searchQuery = new SearchQuery();
                searchQuery.GetTotalCount = true;
                searchQuery.Query = new MatchAllQuery();
                var request = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery);

                var response = OTSClient.Search(request);
                if (response.TotalCount == ExceptionRowCount)
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

        public AggregationResults TestAggregationBase(params IAggregation[] aggregations)
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

            SearchRequest searchRequest = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = OTSClient.Search(searchRequest);

            return response.AggregationResults;
        }

        public GroupByResults TestGroupByBase(params IGroupBy[] groupBys)
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

            SearchRequest searchRequest = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = OTSClient.Search(searchRequest);

            return response.GroupByResults;
        }

        public static void AssertFieldSchema(FieldSchema except, FieldSchema actual)
        { 
            if (except == null && actual == null)
            {
                return;
            }

            if (except == null || actual == null)
            {
                Assert.Fail();
            }

            Assert.AreEqual(except.FieldName, actual.FieldName);
            Assert.AreEqual(except.DateFormats, actual.DateFormats);
            Assert.AreEqual(except.EnableSortAndAgg, actual.EnableSortAndAgg);
            Assert.AreEqual(except.FieldType, actual.FieldType);
            Assert.AreEqual(except.IsVirtualField, actual.IsVirtualField);
            Assert.AreEqual(except.IsArray, actual.IsArray);
            Assert.AreEqual(except.SourceFieldNames, actual.SourceFieldNames);
            Assert.AreEqual(except.Store, actual.Store);
            Assert.AreEqual(except.SubFieldSchemas, actual.SubFieldSchemas);
        }

        public static void AssertPercentilesResultItems(List<PercentilesAggregationResultItem> except, List<PercentilesAggregationResultItem> actual)
        { 
            if (except == null && actual == null)
            {
                return;
            }

            if (except == null || actual == null || except.Count != actual.Count)
            {
                Assert.Fail();
            }

            for (int i = 0; i < except.Count; i++)
            {
                Assert.LessOrEqual(Math.Abs(except[i].Key - actual[i].Key), 0.00001);
                Assert.LessOrEqual(Math.Abs(except[i].Value.AsDouble() - actual[i].Value.AsDouble()), 0.00001);
            }
        }
    }
}
