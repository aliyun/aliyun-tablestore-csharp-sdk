using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Samples
{
    public static class MultiRowReadWriteSample
    {

        private static string TableName = "multiRowReadWriteSample";

        private static void PrepareTable()
        {
            // 创建表
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(TableName)) {
                return;
            }


            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("pk0", ColumnValueType.Integer);
            primaryKeySchema.Add("pk1", ColumnValueType.String);
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

        }

        private static void PrepareData()
        {
            OTSClient otsClient = Config.GetClient();

            // 插入100条数据
            for (int i = 0; i < 100; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.Add("pk0", new ColumnValue(i));
                primaryKey.Add("pk1", new ColumnValue("abc"));

                // 定义要写入改行的属性列
                AttributeColumns attribute = new AttributeColumns();
                attribute.Add("col0", new ColumnValue(0));
                attribute.Add("col1", new ColumnValue("a"));
                attribute.Add("col2", new ColumnValue(i % 3 != 0));
                PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);

                otsClient.PutRow(request);
            }
        }

        public static void GetRange()
        {
            Console.WriteLine("Start get range...");

            PrepareTable();
            PrepareData();

            OTSClient otsClient = Config.GetClient();
            // 读取 (0, INF_MIN)到(100, INF_MAX)这个范围内的所有行
            PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey();
            inclusiveStartPrimaryKey.Add("pk0", new ColumnValue(0));
            inclusiveStartPrimaryKey.Add("pk1", ColumnValue.INF_MIN);

            PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey();
            exclusiveEndPrimaryKey.Add("pk0", new ColumnValue(100));
            exclusiveEndPrimaryKey.Add("pk1", ColumnValue.INF_MAX);
            GetRangeRequest request = new GetRangeRequest(TableName, GetRangeDirection.Forward, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey);

            GetRangeResponse response = otsClient.GetRange(request);
            IList<RowDataFromGetRange> rows = response.RowDataList;
            PrimaryKey nextStartPrimaryKey = response.NextPrimaryKey;
            while (nextStartPrimaryKey != null)
            {
                request = new GetRangeRequest(TableName, GetRangeDirection.Forward, nextStartPrimaryKey, exclusiveEndPrimaryKey);
                response = otsClient.GetRange(request);
                nextStartPrimaryKey = response.NextPrimaryKey;
                foreach (RowDataFromGetRange row in response.RowDataList)
                {
                    rows.Add(row);
                }
            }

            foreach (RowDataFromGetRange row in rows)
            {
                Console.WriteLine("-----------------");
                foreach (KeyValuePair<string, ColumnValue> entry in row.PrimaryKey)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                foreach (KeyValuePair<string, ColumnValue> entry in row.Attribute)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                Console.WriteLine("-----------------");
            }

            Console.WriteLine("TotalRowsRead: " + rows.Count);
        }

        public static void GetRangeWithFilter()
        {
            Console.WriteLine("Start get range with filter ...");
            PrepareTable();
            PrepareData();

            OTSClient otsClient = Config.GetClient();
            // 读取 (0, INF_MIN)到(100, INF_MAX)这个范围内的所有行，且col2等于false的行
            PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey();
            inclusiveStartPrimaryKey.Add("pk0", new ColumnValue(0));
            inclusiveStartPrimaryKey.Add("pk1", ColumnValue.INF_MIN);

            PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey();
            exclusiveEndPrimaryKey.Add("pk0", new ColumnValue(100));
            exclusiveEndPrimaryKey.Add("pk1", ColumnValue.INF_MAX);

            // 构造列过滤条件
            var condition = new RelationalCondition("col2",
                                RelationalCondition.CompareOperator.EQUAL,
                                new ColumnValue(false));
           
            var queryCriteria = new RangeRowQueryCriteria(TableName)
            {
                Direction = GetRangeDirection.Forward,
                InclusiveStartPrimaryKey = inclusiveStartPrimaryKey,
                ExclusiveEndPrimaryKey = exclusiveEndPrimaryKey,
                Filter = condition
            };

            GetRangeResponse response = otsClient.GetRange(new GetRangeRequest(queryCriteria));
            IList<RowDataFromGetRange> rows = response.RowDataList;
            PrimaryKey nextStartPrimaryKey = response.NextPrimaryKey;
            while (nextStartPrimaryKey != null)
            {
                queryCriteria = new RangeRowQueryCriteria(TableName)
                {
                    Direction = GetRangeDirection.Forward,
                    InclusiveStartPrimaryKey = nextStartPrimaryKey,
                    ExclusiveEndPrimaryKey = exclusiveEndPrimaryKey,
                    Filter = condition
                };

                response = otsClient.GetRange(new GetRangeRequest(queryCriteria));
                nextStartPrimaryKey = response.NextPrimaryKey;
                foreach (RowDataFromGetRange row in response.RowDataList)
                {
                    rows.Add(row);
                }
            }

            foreach (RowDataFromGetRange row in rows)
            {
                Console.WriteLine("-----------------");
                foreach (KeyValuePair<string, ColumnValue> entry in row.PrimaryKey)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                foreach (KeyValuePair<string, ColumnValue> entry in row.Attribute)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                Console.WriteLine("-----------------");
            }

            Console.WriteLine("TotalRowsRead with filter: " + rows.Count);
        }

        public static void GetIterator()
        {
            Console.WriteLine("Start get iterator...");

            PrepareTable();
            PrepareData();

            OTSClient otsClient = Config.GetClient();
            // 读取 (0, "a")到(1000, "xyz")这个范围内的所有行
            PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey();
            inclusiveStartPrimaryKey.Add("pk0", new ColumnValue(0));
            inclusiveStartPrimaryKey.Add("pk1", new ColumnValue("a"));

            PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey();
            exclusiveEndPrimaryKey.Add("pk0", new ColumnValue(1000));
            exclusiveEndPrimaryKey.Add("pk1", new ColumnValue("xyz"));

            var cu = new CapacityUnit(0, 0);
            var request = new GetIteratorRequest(TableName, GetRangeDirection.Forward, inclusiveStartPrimaryKey, 
                                                exclusiveEndPrimaryKey, cu);

            var iterator = otsClient.GetRangeIterator(request);
            foreach (var row in iterator)
            {
                Console.WriteLine("-----------------");
                foreach (KeyValuePair<string, ColumnValue> entry in row.PrimaryKey)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                foreach (KeyValuePair<string, ColumnValue> entry in row.Attribute)
                {
                    Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                }
                Console.WriteLine("-----------------");
            }

            Console.WriteLine("Consumed CapacityUnit Counter:{0}", cu.Read);
        }

        public static void BatchGetRow()
        {
            Console.WriteLine("Start batch get row...");
            PrepareTable();
            PrepareData();
            OTSClient otsClient = Config.GetClient();

            // 批量一次读10行
            BatchGetRowRequest request = new BatchGetRowRequest();
            List<PrimaryKey> primaryKeys = new List<PrimaryKey>();
            for (int i = 0; i < 10; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.Add("pk0", new ColumnValue(i));
                primaryKey.Add("pk1", new ColumnValue("abc"));
                primaryKeys.Add(primaryKey);
            }

            request.Add(TableName, primaryKeys);

            BatchGetRowResponse response = otsClient.BatchGetRow(request);
            var tableRows = response.RowDataGroupByTable;
            var rows = tableRows[TableName];

            foreach(var row in rows)
            {
                // 注意：batch操作可能部分成功部分失败，需要为每行检查状态
                if (row.IsOK)
                {
                    Console.WriteLine("-----------------");
                    foreach (KeyValuePair<string, ColumnValue> entry in row.PrimaryKey)
                    {
                        Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                    }
                    foreach (KeyValuePair<string, ColumnValue> entry in row.Attribute)
                    {
                        Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                    }
                    Console.WriteLine("-----------------");
                } else
                {
                    Console.WriteLine("Read row failed: " + row.ErrorMessage);
                }
            }

            Console.WriteLine("RowsCount: " + rows.Count);
        }

        public static void BatchGetRowWithFilter()
        {
            Console.WriteLine("Start batch get row with filter ...");
            PrepareTable();
            PrepareData();
            OTSClient otsClient = Config.GetClient();

            // 批量一次读10行
            BatchGetRowRequest request = new BatchGetRowRequest();
            List<PrimaryKey> primaryKeys = new List<PrimaryKey>();
            for (int i = 0; i < 10; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.Add("pk0", new ColumnValue(i));
                primaryKey.Add("pk1", new ColumnValue("abc"));
                primaryKeys.Add(primaryKey);
            }

            var condition = new RelationalCondition("col2",
                    RelationalCondition.CompareOperator.EQUAL,
                    new ColumnValue(false));

            request.Add(TableName, primaryKeys, null, condition);

            BatchGetRowResponse response = otsClient.BatchGetRow(request);
            var tableRows = response.RowDataGroupByTable;
            var rows = tableRows[TableName];

            foreach (var row in rows)
            {
                // 注意：batch操作可能部分成功部分失败，需要为每行检查状态
                if (row.IsOK)
                {
                    Console.WriteLine("-----------------");
                    foreach (KeyValuePair<string, ColumnValue> entry in row.PrimaryKey)
                    {
                        Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                    }
                    foreach (KeyValuePair<string, ColumnValue> entry in row.Attribute)
                    {
                        Console.WriteLine(entry.Key + ":" + PrintColumnValue(entry.Value));
                    }
                    Console.WriteLine("-----------------");
                }
                else
                {
                    Console.WriteLine("Read row with filter failed: " + row.ErrorMessage);
                }
            }

            Console.WriteLine("RowsCount with filter");
        }

        public static void BatchWriteRow()
        {
            Console.WriteLine("Start batch write row...");
            PrepareTable();
            PrepareData();
            OTSClient otsClient = Config.GetClient();

            // 一次批量导入100行数据
            var request = new BatchWriteRowRequest();
            var rowChanges = new RowChanges();
            for (int i = 0; i < 100; i++)
            {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.Add("pk0", new ColumnValue(i));
                primaryKey.Add("pk1", new ColumnValue("abc"));

                // 定义要写入改行的属性列
                UpdateOfAttribute attribute = new UpdateOfAttribute();
                attribute.AddAttributeColumnToPut("col0", new ColumnValue(0));
                attribute.AddAttributeColumnToPut("col1", new ColumnValue("a"));
                attribute.AddAttributeColumnToPut("col2", new ColumnValue(true));

                rowChanges.AddUpdate(new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);
            }

            request.Add(TableName, rowChanges);

            var response = otsClient.BatchWriteRow(request);
            var tableRows = response.TableRespones;
            var rows = tableRows[TableName];

            int succeedRows = 0;
            int failedRows = 0;
            foreach (var row in rows.UpdateResponses)
            {
                // 注意：batch操作可能部分成功部分失败，需要为每行检查状态
                if (row.IsOK)
                {
                    Console.WriteLine("Update succeed.");
                    succeedRows++;
                }
                else
                {
                    Console.WriteLine("Read row failed: " + row.ErrorMessage);
                    failedRows++;
                }
            }

            Console.WriteLine("SucceedRows: " + succeedRows);
            Console.WriteLine("FailedRows: " + failedRows);
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
    }
}
