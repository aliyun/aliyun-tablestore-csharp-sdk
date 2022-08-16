using Aliyun.OTS.DataModel.SQL;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    public class SQLTest : SearchUnitTestBase
    {
        [Test]
        public void TestSQLQuery()
        {
            string query = "select * from " + TestTableName;

            SQLQueryRequest request = new SQLQueryRequest(query);
            SQLQueryResponse response = OTSClient.SQLQuery(request);

            Console.WriteLine(JsonConvert.SerializeObject(response.GetSQLResultSet().GetSQLTableMeta()));
            Console.WriteLine(response.SQLStatementType.ToString());

            ISQLResultSet selectResultSet = response.GetSQLResultSet();
            while (selectResultSet.HasNext())
            {
                ISQLRow row = selectResultSet.Next();

                Console.WriteLine(Convert.ToBase64String(row.GetBinary("col4").ToArray()));

                Console.WriteLine(row.ToDebugString());
            }

            query = "show tables";
            request.Query = query;

            response = OTSClient.SQLQuery(request);

            Console.WriteLine(JsonConvert.SerializeObject(response.GetSQLResultSet().GetSQLTableMeta()));
            Console.WriteLine(response.SQLStatementType.ToString());

            ISQLResultSet showtableResultSet = response.GetSQLResultSet();

            while (showtableResultSet.HasNext())
            {
                ISQLRow row = showtableResultSet.Next();

                Console.WriteLine(row.ToDebugString());
            }

            request = new SQLQueryRequest("show index in " + TestTableName);
            response = OTSClient.SQLQuery(request);

            Console.WriteLine(response.SQLStatementType.ToString());
            Console.WriteLine(JsonConvert.SerializeObject(response.GetSQLResultSet().GetSQLTableMeta()));

            ISQLResultSet showIndexResultSet = response.GetSQLResultSet();

            while (showIndexResultSet.HasNext())
            {
                ISQLRow row = showIndexResultSet.Next();

                Console.WriteLine(row.ToDebugString());
            }
        }
    }
}
