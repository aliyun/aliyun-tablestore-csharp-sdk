using System;
using System.Collections.Generic;
using System.Threading;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;

namespace Aliyun.OTS.Samples.Samples
{
    public class SearchIndexGeoSample
    {
        private static readonly string TableName = "SearchIndexGeoSampleTable";
        private static readonly string IndexName = "SearchIndexGeoSampleTableIndex";
        private static readonly string Pk0 = "pk0";
        private static readonly string Pk1 = "pk1";
        private static readonly string Geo_type_col = "geo_type_col";

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

            //Wait searchIndex load success
            WaiteAllDataSyncSuccess(otsClient, 10);

            //GeoBoundingBoxQuery
            GeoBoundingBoxQuery(otsClient);

            //GeoDistanceQuery
            GeoDistanceQuery(otsClient);

            //GeoPolygonQuery
            GeoPolygonQuery(otsClient);

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

            CreateSearchIndexRequest request = new CreateSearchIndexRequest(TableName,IndexName);
            List<FieldSchema> FieldSchemas = new List<FieldSchema>() {
                new FieldSchema(Geo_type_col,FieldType.GEO_POINT){ index=true,EnableSortAndAgg=true},
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
                "0,0",
                "7,3",
                "2,8",
                "5,9",
                "10,10",
                "20,20",
                "14,18",
                "12,16",
                "15,4",
                "8,13"
            };
            for (int i = 0; i < 10; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey{
                    { Pk0, new ColumnValue(i) },
                    { Pk1, new ColumnValue("pk1value") }
                };
                AttributeColumns attribute = new AttributeColumns{
                    { Geo_type_col, new ColumnValue(colList[i]) }
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
            var request = new SearchRequest(TableName, IndexName, searchQuery);

            var response = otsClient.Search(request);

            Console.WriteLine("IsAllSuccess:" + response.IsAllSuccess);
            Console.WriteLine("Total Count:" + response.TotalCount);
        }

        /// <summary>
        /// geo_type_col是GeoPoint类型，查询表中geo_type_col这一列的值在左上角为"10,0", 右下角为"0,10"的矩形范围内的数据
        /// </summary>
        /// <param name="client"></param>
        public static void GeoBoundingBoxQuery(OTSClient client)
        {
            Console.WriteLine("\n Start GeoBoundingBox query...");

            SearchQuery searchQuery = new SearchQuery();
            GeoBoundingBoxQuery geoBoundingBoxQuery = new GeoBoundingBoxQuery(); // 设置查询类型为GeoBoundingBoxQuery
            geoBoundingBoxQuery.FieldName = Geo_type_col; // 设置比较哪个字段的值
            geoBoundingBoxQuery.TopLeft = "10,0"; // 设置矩形左上角
            geoBoundingBoxQuery.BottomRight = "0,10"; // 设置矩形右下角
            searchQuery.Query = geoBoundingBoxQuery;

            SearchRequest searchRequest = new SearchRequest(TableName, IndexName, searchQuery);

            var columnsToGet = new ColumnsToGet();
            columnsToGet.Columns = new List<string> { Geo_type_col };  //设置返回Col_GeoPoint这一列
            searchRequest.ColumnsToGet = columnsToGet;

            SearchResponse response = client.Search(searchRequest);
            Console.WriteLine(response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }

        /// <summary> 
        ///  查询表中geo_type_col这一列的值距离中心点不超过一定距离的数据。
        /// </summary>
        /// <param name="client"></param>
        public static void GeoDistanceQuery(OTSClient client)
        {
            Console.WriteLine("\n Start GeoDistance query...");

            SearchQuery searchQuery = new SearchQuery();
            GeoDistanceQuery geoDistanceQuery = new GeoDistanceQuery();  // 设置查询类型为GeoDistanceQuery
            geoDistanceQuery.FieldName = Geo_type_col;
            geoDistanceQuery.CenterPoint = "10,11"; // 设置中心点
            geoDistanceQuery.DistanceInMeter = 10000; // 设置到中心点的距离条件，不超过50米
            searchQuery.Query = geoDistanceQuery;

            SearchRequest searchRequest = new SearchRequest(TableName, IndexName, searchQuery);

            ColumnsToGet columnsToGet = new ColumnsToGet();
            columnsToGet.Columns = new List<string>() { Geo_type_col };  //设置返回Col_GeoPoint这一列
            searchRequest.ColumnsToGet = columnsToGet;

            SearchResponse response = client.Search(searchRequest);
            Console.WriteLine(response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }


        /// <summary>
        /// 查询表中geo_type_col这一列的值在一个给定多边形范围内的数据。
        /// </summary>
        /// <param name="client"></param>
        public static void GeoPolygonQuery(OTSClient client)
        {
            Console.WriteLine("\n Start GeoPolygon query...");

            SearchQuery searchQuery = new SearchQuery();
            GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();  // 设置查询类型为GeoPolygonQuery
            geoPolygonQuery.FieldName = Geo_type_col;
            geoPolygonQuery.Points = new List<string>() { "0,0", "10,0", "10,10" }; // 设置多边形的顶点
            searchQuery.Query = geoPolygonQuery;

            SearchRequest searchRequest = new SearchRequest(TableName, IndexName, searchQuery);

            ColumnsToGet columnsToGet = new ColumnsToGet();
            columnsToGet.Columns = new List<string>() { Geo_type_col };  //设置返回Col_GeoPoint这一列
            searchRequest.ColumnsToGet = columnsToGet;

            SearchResponse response = client.Search(searchRequest);
            Console.WriteLine(response.TotalCount);
            foreach (var row in response.Rows)
            {
                PrintRow(row);
            }
        }
    }
}
