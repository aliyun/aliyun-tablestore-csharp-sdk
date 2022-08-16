using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.GroupBy;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class SearchTest : SearchUnitTestBase
    {
        [Test]
        public void TestTimeoutinRequest()
        {
            string TableName = TestTableName;
            string IndexName = TestSearchIndexName;

            var searchQuery = new SearchQuery();
            searchQuery.Query = new MatchAllQuery();
            searchQuery.GetTotalCount = true;

            var request = new SearchRequest(TableName, IndexName, searchQuery);
            request.TimeoutInMillisecond = 5000;

            var response = OTSClient.Search(request);
            Console.WriteLine("IsAllSuccess:" + response.IsAllSuccess);
            Console.WriteLine("Total Count:" + response.TotalCount);
        }

        [Test]
        public void TestDescribeSearchRequest()
        {
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest(TestTableName, TestSearchIndexName);
            DescribeSearchIndexResponse describeSearchIndexResponse = OTSClient.DescribeSearchIndex(describeSearchIndexRequest);

            //打印response详细信息。
            Console.WriteLine(JsonConvert.SerializeObject(describeSearchIndexResponse));

            Assert.AreEqual(ExceptionTimeToLine, describeSearchIndexResponse.TimeToLive);

            foreach (FieldSchema schema in describeSearchIndexResponse.Schema.FieldSchemas)
            {
                if (schema.FieldName == Col0Virtual2Schema.FieldName)
                {
                    AssertFieldSchema(Col0Virtual2Schema, schema);
                }
                else if (schema.FieldName == Col0VirtualSchema.FieldName)
                {
                    AssertFieldSchema(Col0VirtualSchema, schema);
                }
                else if (schema.FieldName == Col1IntegerSchema.FieldName)
                {
                    AssertFieldSchema(Col1IntegerSchema, schema);
                }
                else if (schema.FieldName == COl2Schema.FieldName)
                {
                    AssertFieldSchema(COl2Schema, schema);
                }
                else if (schema.FieldName == Pk0Schema.FieldName)
                {
                    AssertFieldSchema(Pk0Schema, schema);
                }
                else if (schema.FieldName == Pk1Schema.FieldName)
                {
                    AssertFieldSchema(Pk1Schema, schema);
                }
            }
        }

        [Test]
        public void TestDescribeTableRequest()
        {
            DescribeTableRequest describeTableRequest = new DescribeTableRequest(TestTableName);

            DescribeTableResponse response = OTSClient.DescribeTable(describeTableRequest);

            Console.WriteLine(JsonConvert.SerializeObject(response));
        }

        [Test]
        public void TestUpdateSearchIndexRequest()
        {
            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest(TestTableName, TestSearchIndexName);
            ExceptionTimeToLine = 14 * 24 * 60 * 60;
            request.SetTimeToLive(ExceptionTimeToLine);
            OTSClient.UpdateSearchIndex(request);
        }

        [Test]
        public void TestComputeSplitsAndParallelScan()
        {
            SearchIndexSplitsOptions options = new SearchIndexSplitsOptions();
            options.IndexName = TestSearchIndexName;

            ComputeSplitsRequest request = new ComputeSplitsRequest(TestTableName, options);

            ComputeSplitsResponse response = OTSClient.ComputeSplits(request);
            Console.WriteLine(JsonConvert.SerializeObject(response));

            ParallelScanRequest scanRequest = new ParallelScanRequest(TestTableName, TestSearchIndexName);

            ExistsQuery Query = new ExistsQuery();
            Query.FieldName = Col0_Virtual1;

            ScanQuery query = new ScanQuery();
            query.AliveTime = 60;
            query.MaxParallel = response.SplitsSize;
            query.Limit = 1;
            query.Query = Query;

            ColumnsToGet columns = new ColumnsToGet();
            columns.ReturnAllFromIndex = true;

            scanRequest.ScanQuery = query;
            scanRequest.ColumnToGet = columns;
            scanRequest.SessionId = response.SessionId;

            int total = 0;
            ParallelScanResponse scanResponse = OTSClient.ParallelScan(scanRequest);

            while (scanResponse.NextToken != null)
            {
                List<Row> rows = new List<Row>(scanResponse.Rows);
                total += rows.Count;

                scanRequest.ScanQuery.Token = scanResponse.NextToken;

                scanResponse = OTSClient.ParallelScan(scanRequest);

                Console.WriteLine(JsonConvert.SerializeObject(scanResponse));
            }

            Assert.AreEqual(ExceptionRowCount, total);

            Console.WriteLine("Total rows : {0}", total);
        }

        [Test]
        public void TestCreateSearchIndexWithVirtualColumn()
        {
            // 创建索引
            CreateSearchIndexRequest createSearchIndexRequest = new CreateSearchIndexRequest(TestTableName, TestSearchVirtualIndexName);
            List<FieldSchema> fieldSchemas = new List<FieldSchema> {
                Col1IntegerSchema,

                Pk1Schema,

                DateSchema,

                Col0VirtualSchema,

                Col0Virtual2Schema,
            };

            createSearchIndexRequest.IndexSchame = new IndexSchema()
            {
                FieldSchemas = fieldSchemas,
                IndexSort = new Sort(
                    new List<ISorter>
                    {
                        new FieldSort(Pk1_Integer, SortOrder.ASC)
                    })
            };

            Console.WriteLine(JsonConvert.SerializeObject(OTSClient.CreateSearchIndex(createSearchIndexRequest)));
        }

        [Test]
        public void TestMatchPhraseQuery()
        {
            SearchQuery searchQuery = new SearchQuery();

            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery(Col0_Virtual1, "TableStore");
            matchPhraseQuery.Weight = 50.0f;

            searchQuery.Query = matchPhraseQuery;
            searchQuery.Offset = 0;
            searchQuery.Limit = 10;
            searchQuery.GetTotalCount = true;

            SearchRequest searchRequest = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery)
            {
                ColumnsToGet = new ColumnsToGet()
                {
                    ReturnAllFromIndex = true
                }
            };

            SearchResponse searchResponse = OTSClient.Search(searchRequest);

            Assert.AreEqual(9, searchResponse.TotalCount);

            Console.WriteLine(JsonConvert.SerializeObject(searchResponse));
        }

        [Test]
        public void TestMatchAllQuery()
        {
            MatchAllQuery matchAllQuery = new MatchAllQuery();

            SearchQuery searchQuery = new SearchQuery
            {
                Query = matchAllQuery,
                GetTotalCount = true,
            };

            SearchRequest searchRequest = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = OTSClient.Search(searchRequest);
            Console.WriteLine(JsonConvert.SerializeObject(response));

            Assert.AreEqual(ExceptionRowCount, response.TotalCount);
        }

        [Test]
        public void TestExistQuery()
        {
            ExistsQuery existQuery = new ExistsQuery();
            existQuery.FieldName = Col0_Virtual1;

            SearchQuery searchQuery = new SearchQuery();
            searchQuery.Query = existQuery;
            searchQuery.GetTotalCount = true;

            SearchRequest searchRequest = new SearchRequest(TestTableName, TestSearchIndexName, searchQuery);
            searchRequest.ColumnsToGet = new ColumnsToGet() { ReturnAll = true };

            SearchResponse response = OTSClient.Search(searchRequest);
            Console.WriteLine(JsonConvert.SerializeObject(response));

            Assert.AreEqual(ExceptionRowCount, response.TotalCount);
        }

        [Test]
        public void TestMinAndMaxAggregation()
        {
            string minAggName = "min_agg";
            string maxAggName = "max_agg";

            MinAggregation minAggregation = new MinAggregation
            {
                AggName = minAggName,
                FieldName = Col2_Double,
                Missing = new ColumnValue(0.5)
            };

            MaxAggregation maxAggregation = new MaxAggregation
            {
                AggName = maxAggName,
                FieldName = Col2_Double,
                Missing = new ColumnValue(100.1)
            };

            AggregationResults results = TestAggregationBase(minAggregation, maxAggregation);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", minAggName, results.GetAsMinAggregationResult(minAggName).Value);
                Console.WriteLine("{0} result: {1}", maxAggName, results.GetAsMaxAggregationResult(maxAggName).Value);
            }

            Assert.AreEqual(ExceptionRowCount - 1, results.GetAsMaxAggregationResult(maxAggName).Value);
            Assert.AreEqual(0, results.GetAsMinAggregationResult(minAggName).Value);
        }

        [Test]
        public void TestCountAggregation()
        {
            string countAggName = "count_agg";
            string distinctCountName = "distinct_count_agg";

            CountAggregation countAggregation = new CountAggregation
            {
                AggName = countAggName,
                FieldName = Col2_Double,
            };

            DistinctCountAggregation distinctCountAggregation = new DistinctCountAggregation
            {
                AggName = distinctCountName,
                FieldName = Pk0_String,
                Missing = new ColumnValue("NONE")
            };

            AggregationResults results = TestAggregationBase(countAggregation, distinctCountAggregation);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", countAggName, results.GetAsCountAggregationResult(countAggName).Value);
                Console.WriteLine("{0} result: {1}", distinctCountName, results.GetAsDistinctCountAggregationResult(distinctCountName).Value);
            }

            Assert.AreEqual(ExceptionRowCount, results.GetAsCountAggregationResult(countAggName).Value);
            Assert.AreEqual(7, results.GetAsDistinctCountAggregationResult(distinctCountName).Value);
        }

        [Test]
        public void TestPercentilesAggregation()
        {
            string aggName = "percentiles_Agg";

            PercentilesAggregation percentilesAggregation = new PercentilesAggregation
            {
                AggName = aggName,
                FieldName = Col2_Double,
                Percentiles = new List<double> { 0.0, 20.0, 60.0, 80.0, 100.0 }
            };

            AggregationResults results = TestAggregationBase(percentilesAggregation);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", aggName, results.GetAsPercentilesAggregationResult(aggName).Serialize());
            }

            List<PercentilesAggregationResultItem> exceptPercentiles = new List<PercentilesAggregationResultItem>
            {
                new PercentilesAggregationResultItem { Key = 0, Value = new ColumnValue(0.0)},
                new PercentilesAggregationResultItem { Key = 20, Value = new ColumnValue(2.8)},
                new PercentilesAggregationResultItem { Key = 60, Value = new ColumnValue(8.4)},
                new PercentilesAggregationResultItem { Key = 80, Value = new ColumnValue(11.2)},
                new PercentilesAggregationResultItem { Key = 100, Value = new ColumnValue(14.0)}
            };

            AssertPercentilesResultItems(exceptPercentiles, results.GetAsPercentilesAggregationResult(aggName).Value);
        }

        [Test]
        public void TestSumAggregation()
        {
            string aggName = "sum_agg";

            SumAggregation sumAggregation = new SumAggregation
            {
                AggName = aggName,
                FieldName = Col2_Double,
                Missing = new ColumnValue(100.1)
            };

            AggregationResults results = TestAggregationBase(sumAggregation);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", aggName, results.GetAsSumAggregationResult(aggName).Value);
            }

            Assert.LessOrEqual(Math.Abs(results.GetAsSumAggregationResult(aggName).Value-105.0) , 0.00001);
        }

        [Test]
        public void TestTopRowsAggregation()
        {
            string aggName = "top_rows_agg";

            TopRowsAggregation topRowsAggregation = new TopRowsAggregation
            {
                AggName = aggName,
                Sort = new Sort(new List<ISorter> { new FieldSort(Pk1_Integer, SortOrder.DESC) }),
                Limit = 1
            };

            AggregationResults results = TestAggregationBase(topRowsAggregation);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", aggName, JsonConvert.SerializeObject(results.GetAsTopRowsAggregationResult(aggName).Rows));
            }

            Assert.AreEqual(14, results.GetAsTopRowsAggregationResult(aggName).Rows[0].PrimaryKey[Pk1_Integer].AsLong());
        }

        [Test]
        public void TestGroupByField()
        {
            string groupByName = "group_by_field";

            GroupByField groupByField = new GroupByField
            {
                GroupByName = groupByName,
                FieldName = Col2_Double,
                GroupBySorters = new List<GroupBySorter>{
                    new GroupBySorter()
                    {
                        RowCountSort = new RowCountSort{ Order = SortOrder.DESC }
                    },
                    new GroupBySorter()
                    {
                        GroupKeySort = new GroupKeySort{ Order = SortOrder.DESC }
                    }
                }
            };

            GroupByResults results = TestGroupByBase(groupByField);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", groupByName, JsonConvert.SerializeObject(results.GetAsGroupByFieldResult(groupByName).GroupByFieldResultItems));
            }

            Assert.AreEqual("14.0", results.GetAsGroupByFieldResult(groupByName).GroupByFieldResultItems[0].Key);
        }

        [Test]
        public void TestGroupByFilter()
        {
            string groupByName = "group_by_filter";

            GroupByFilter groupByFilter = new GroupByFilter
            {
                GroupByName = groupByName,
                Filters = new List<IQuery>
                {
                    new RangeQuery
                    {
                        FieldName = Col2_Double,
                        From = new ColumnValue(0.0),
                        To = new ColumnValue(100.0)
                    },
                }
            };

            GroupByResults results = TestGroupByBase(groupByFilter);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", groupByName, JsonConvert.SerializeObject(results.GetAsGroupByFilterResult(groupByName).GroupByFilterResultItems));
            }

            Assert.AreEqual(14, results.GetAsGroupByFilterResult(groupByName).GroupByFilterResultItems[0].RowCount);
        }

        [Test]
        public void TestGroupByGeoDistance()
        {
            string groupByName = "group_by_geo_distance";

            GroupByGeoDistance groupByGeoDistance = new GroupByGeoDistance
            {
                GroupByName = groupByName,
                FieldName = Geo_Column,
                Origin = new GeoPoint(0, 0),
                Ranges = new List<Range>
                {
                    new Range(double.MinValue , 1000.0),
                    new Range(1000.0, 5000.0),
                    new Range(5000.0, double.MaxValue)
                }
            };

            GroupByResults results = TestGroupByBase(groupByGeoDistance);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", groupByName, JsonConvert.SerializeObject(results.GetAsGroupByGeoDistanceResult(groupByName).GroupByGeoDistanceResultItems));
            }

            Assert.AreEqual(15, results.GetAsGroupByGeoDistanceResult(groupByName).GroupByGeoDistanceResultItems[2].RowCount);
        }

        [Test]
        public void TestGroupByHistogram()
        {
            string groupByName = "group_by_histogram";

            GroupByHistogram groupByHistogram = new GroupByHistogram
            {
                GroupByName = groupByName,
                FieldName = Col2_Double,
                Interval = new ColumnValue(10.0),
                FieldRange = new FieldRange
                {
                    Min = new ColumnValue(0.0),
                    Max = new ColumnValue(1000.0),
                }
            };

            GroupByResults results = TestGroupByBase(groupByHistogram);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", groupByName, JsonConvert.SerializeObject(results.GetAsGroupByHistogramResult(groupByName).GroupByHistogramResultItems));
            }

            Assert.AreEqual(10, results.GetAsGroupByHistogramResult(groupByName).GroupByHistogramResultItems[0].Value);
            Assert.AreEqual(5, results.GetAsGroupByHistogramResult(groupByName).GroupByHistogramResultItems[1].Value);
        }

        [Test]
        public void TestGroupByRange()
        {
            string groupByName = "group_by_range";

            GroupByRange groupByRange = new GroupByRange();
            groupByRange.FieldName = Col2_Double;
            groupByRange.GroupByName = groupByName;
            groupByRange.Ranges = new List<Range>
            {
                new Range(double.MinValue, 5.0),
                new Range(5.0, 50.0),
                new Range(50.0, 100.0),
                new Range(100.0, double.MaxValue)
            };

            GroupByResults results = TestGroupByBase(groupByRange);

            if (results != null)
            {
                Console.WriteLine("{0} result: {1}", groupByName, JsonConvert.SerializeObject(results.GetAsGroupByRangeResult(groupByName).GroupByRangeResultItems));
            }

            Assert.AreEqual(5, results.GetAsGroupByRangeResult(groupByName).GroupByRangeResultItems[0].RowCount);
            Assert.AreEqual(10, results.GetAsGroupByRangeResult(groupByName).GroupByRangeResultItems[1].RowCount);
            Assert.AreEqual(0, results.GetAsGroupByRangeResult(groupByName).GroupByRangeResultItems[2].RowCount);
        }
    }
}
