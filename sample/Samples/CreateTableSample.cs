using System;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using System.Threading;

namespace Aliyun.OTS.Samples
{
    public static class CreateTableSample
    {

        private static string TableName = "createTableSample";

        public static void TableOperations()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();
            {
                Console.WriteLine("Start create table...");
                PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema();
                primaryKeySchema.Add("pk0", ColumnValueType.Integer);
                primaryKeySchema.Add("pk1", ColumnValueType.String);
                TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

                CapacityUnit reservedThroughput = new CapacityUnit(1, 1);
                CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
                otsClient.CreateTable(request);

                Console.WriteLine("Table is created: " + TableName);
            }

            // 更新表
            {
                Thread.Sleep(60 * 1000); // 每次更新表需要至少间隔1分钟
                Console.WriteLine("Start update table...");
                CapacityUnit reservedThroughput = new CapacityUnit(0, 0); // 将预留CU调整为0，0
                UpdateTableRequest request = new UpdateTableRequest(TableName, reservedThroughput);
                UpdateTableResponse response = otsClient.UpdateTable(request);
                Console.WriteLine("LastIncreaseTime: " + response.ReservedThroughputDetails.LastIncreaseTime);
                Console.WriteLine("LastDecreaseTime: " + response.ReservedThroughputDetails.LastDecreaseTime);
                Console.WriteLine("NumberOfDecreaseToday: " + response.ReservedThroughputDetails.LastIncreaseTime);
                Console.WriteLine("ReadCapacity: " + response.ReservedThroughputDetails.CapacityUnit.Read);
                Console.WriteLine("WriteCapacity: " + response.ReservedThroughputDetails.CapacityUnit.Write);
            }

            // 描述表
            {
                Console.WriteLine("Start describe table...");
                DescribeTableRequest request = new DescribeTableRequest(TableName);
                DescribeTableResponse response = otsClient.DescribeTable(request);
                Console.WriteLine("LastIncreaseTime: " + response.ReservedThroughputDetails.LastIncreaseTime);
                Console.WriteLine("LastDecreaseTime: " + response.ReservedThroughputDetails.LastDecreaseTime);
                Console.WriteLine("NumberOfDecreaseToday: " + response.ReservedThroughputDetails.LastIncreaseTime);
                Console.WriteLine("ReadCapacity: " + response.ReservedThroughputDetails.CapacityUnit.Read);
                Console.WriteLine("WriteCapacity: " + response.ReservedThroughputDetails.CapacityUnit.Write);
            }

            // 删除表
            {
                Console.WriteLine("Start delete table...");
                DeleteTableRequest request = new DeleteTableRequest(TableName);
                otsClient.DeleteTable(request);
                Console.WriteLine("Table is deleted.");
            }
        }
    }
}
