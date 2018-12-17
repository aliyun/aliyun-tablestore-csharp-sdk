/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;
using Aliyun.OTS.Handler;
using Aliyun.OTS.DataModel.ConditionalUpdate;
using System;
using System.Net;
using System.Net.Http;

namespace Aliyun.OTS
{
    /// <summary>
    /// OTSClient实现，用来访问OTS。
    /// <para>
    /// OTSClient实现了OTS服务的所有接口，包括同步形式和异步形式。用户可以通过创建OTSClient的实例，并调用它的
    /// 方法来访问OTS服务的所有功能。
    /// </para>
    /// <para>
    /// 除了<see cref="GetRangeIterator"/>外，OTSClient提供的其他API都存在2种形式：同步和异步；并且
    /// 这些接口都接受一个特定的request实例，这个实例分别封装了请求的不同参数；同样，这些接口都返回一个
    /// response实例，这个实例封装了API的返回数据。
    /// </para>
    /// <para>
    /// 例如<see cref="GetRow"/>接口，它接受<see cref="GetRowRequest"/>的实例作为参数，在这里实例里
    /// 你可以指定表名、主键名以及其他参数；这个接口返回<see cref="GetRowResponse"/>的实例，包含本次
    /// GetRow请求消耗的CapacityUnit。
    /// </para>
    /// <para>
    /// 而 <see cref="GetRow"/> 的异步形式 <see cref="GetRowAsync"/> 同样接受 <see cref="GetRowRequest"/>
    /// 作为参数，但它返回一个Task实例，用来代表异步操作。这个异步操作的结果（Task.Result）即
    /// <see cref="GetRowResponse"/>。
    /// </para>
    /// </summary>
    public class OTSClient
    {
        #region Fields & Properties

        private HttpClient client;

        private OTSHandler OTSHandler;
        private readonly OTSClientConfig ClientConfig;

        #endregion

        #region Construct

        /// <summary>
        /// OTSClient的构造函数。
        /// </summary>
        /// <param name="endPoint">OTS服务的地址（例如 'http://instance.cn-hangzhou.ots.aliyun.com:80'），必须以'http://'或者'https://'开头。</param>
        /// <param name="accessKeyID">OTS的Access Key ID，通过官方网站申请。</param>
        /// <param name="accessKeySecret">OTS的Access Key Secret，通过官方网站申请。</param>
        /// <param name="instanceName">OTS实例名，通过官方网站控制台创建。</param>
        /// 
        public OTSClient(string endPoint, string accessKeyID, string accessKeySecret, string instanceName)
            : this(new OTSClientConfig(endPoint, accessKeyID, accessKeySecret, instanceName)) { }

        // public OTSClient(string configFileName) { }
        // TODO enable client config file later

        /// <summary>
        /// 通过客户端配置<see cref="OTSClientConfig"/>的实例来创建<see cref="OTSClient"/>实例。
        /// </summary>
        /// <param name="config">客户端配置实例</param>
        public OTSClient(OTSClientConfig config)
        {
            ClientConfig = config;
            OTSHandler = new OTSHandler();

            client = new HttpClient
            {
                BaseAddress = new Uri(ClientConfig.EndPoint)
            };

            ServicePointManager.DefaultConnectionLimit = config.ConnectionLimit;
            OTSClientTestHelper.Reset();
        }

        #endregion

        #region Table Operations
        // ListTable
        /// <summary>
        /// 获取当前实例下已创建的所有表的表名。
        /// </summary>
        /// <param name="request">请求参数</param>
        /// <returns>ListTable的返回，用来获取表名列表。</returns>
        /// <example>
        /// 获取一个实例下所有表名并循环读取
        /// <code lang="C#">            
        /// var request = new ListTableRequest();
        /// var response = otsClient.ListTable(request);
        /// foreach (var tableName in response.TableNames) {
        ///      // Do something
        /// }
        /// </code>
        /// </example>
        public ListTableResponse ListTable(ListTableRequest request)
        {
            return GetResponseFromAsyncTask(ListTableAsync(request));
        }

        /// <summary>
        /// ListTable的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<ListTableResponse> ListTableAsync(ListTableRequest request)
        {
            return CallAsync<ListTableRequest, ListTableResponse>("/ListTable", request);
        }

        /// <summary>
        /// 根据表信息（包含表名、主键的设计和预留读写吞吐量）创建表。
        /// </summary>
        /// <param name="request">请求参数</param>
        /// <returns>CreateTable的返回，这个返回实例是空的，不包含具体信息。
        /// </returns>
        /// <example>
        /// 创建一个有2个主键列，预留读吞吐量为0，预留写吞吐量为0的表。CU的详细使用规则请参考OTS文档
        /// <code>
        /// var primaryKeySchema = new PrimaryKeySchema();
        /// primaryKeySchema.Add("PK0", ColumnValueType.Integer);
        /// primaryKeySchema.Add("PK1", ColumnValueType.String);
        /// 
        /// var tableMeta = new TableMeta("SampleTable", primaryKeySchema);
        /// var reservedThroughput = new CapacityUnit(0, 0);
        /// var request = new CreateTableRequest(tableMeta, reservedThroughput);
        /// var response = otsClient.CreateTable(request);
        /// </code>
        /// </example>
        public CreateTableResponse CreateTable(CreateTableRequest request)
        {
            return GetResponseFromAsyncTask(CreateTableAsync(request));
        }

        /// <summary>
        /// CreateTable的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request)
        {
            return CallAsync<CreateTableRequest, CreateTableResponse>("/CreateTable", request);
        }

        /// <summary>
        /// 根据表名删除表。
        /// </summary>
        /// <param name="request">请求参数</param>
        /// <returns>DeleteTable的返回，这个返回实例是空的，不包含具体信息。
        /// </returns>
        public DeleteTableResponse DeleteTable(DeleteTableRequest request)
        {
            return GetResponseFromAsyncTask(DeleteTableAsync(request));
        }

        /// <summary>
        /// DeleteTable的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request)
        {
            return CallAsync<DeleteTableRequest, DeleteTableResponse>("/DeleteTable", request);
        }

        // UpdateTable
        /// <summary>
        /// 更新指定表的预留读吞吐量或预留写吞吐量设置，新设定将于更新成功一分钟内生效。
        /// </summary>
        /// <param name="request">请求参数，包含表名以及预留读写吞吐量</param>
        /// <returns>UpdateTable的返回，包含更新后的预留读写吞吐量等信息</returns>
        /// <example>
        /// 将表的预留读吞吐量调整为1，预留写吞吐量调整为1。
        /// <code>
        /// var reservedThroughput = new CapacityUnit(1, 1);
        /// var request = new UpdateTableRequest("SampleTable", reservedThroughput);
        /// var response = otsClient.UpdateTable(request);
        /// </code>
        /// </example>
        public UpdateTableResponse UpdateTable(UpdateTableRequest request)
        {
            return GetResponseFromAsyncTask(UpdateTableAsync(request));
        }

        /// <summary>
        /// UpdateTable的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request)
        {
            return CallAsync<UpdateTableRequest, UpdateTableResponse>("/UpdateTable", request);
        }

        // DescribeTable
        /// <summary>
        /// 查询指定表的结构信息和预留读写吞吐量设置信息。
        /// </summary>
        /// <param name="request">请求参数，包含表名</param>
        /// <returns>DescribeTable的返回，包含表的结构信息和预留读写吞吐量等信息。</returns>
        public DescribeTableResponse DescribeTable(DescribeTableRequest request)
        {
            return GetResponseFromAsyncTask(DescribeTableAsync(request));
        }

        /// <summary>
        /// DescribeTable的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request)
        {
            return CallAsync<DescribeTableRequest, DescribeTableResponse>("/DescribeTable", request);
        }

        #endregion

        #region Single Row Operations
        /// <summary>
        /// 根据给定的主键读取单行数据。
        /// </summary>
        /// <param name="request">请求参数</param>
        /// <returns>GetRow的返回</returns>
        /// <example>
        /// <code>
        /// var primaryKey = new PrimaryKey();
        /// primaryKey.Add("PK0", new ColumnValue("ABC"));
        /// primaryKey.Add("PK1", new ColumnValue(123));
        /// var getRowRequest = new GetRowRequest(
        ///     "SampleTableName",
        ///     primaryKey
        /// );
        /// var getRowResponse = otsClient.GetRow(getRowRequest);
        /// 
        /// System.Console.WriteLine("GetRow CU Consumed: Read {0} Write {0}",
        ///                          getRowResponse.ConsumedCapacityUnit.Read,
        ///                          getRowResponse.ConsumedCapacityUnit.Write);
        /// 
        /// var pk0 = getRowResponse.PrimaryKey["PK0"];
        /// System.Console.WriteLine("PrimaryKey PK0 Value {0}", pk0.StringValue);
        /// var pk1 = getRowResponse.PrimaryKey["PK1"];
        /// System.Console.WriteLine("PrimaryKey PK1 Value {0}", pk1.IntegerValue);
        /// var attr0 = getRowResponse.Attribute["IntAttr0"];
        /// System.Console.WriteLine("Attribute IntAttr0 Value {0}", attr0.IntegerValue);
        /// var attr1 = getRowResponse.Attribute["StringAttr1"];
        /// System.Console.WriteLine("Attribute StringAttr1 Value {0}", attr1.StringValue);
        /// var attr2 = getRowResponse.Attribute["DoubleAttr2"];
        /// System.Console.WriteLine("Attribute DoubleAttr2 Value {0}", attr2.DoubleValue);
        /// var attr3 = getRowResponse.Attribute["BooleanAttr3"];
        /// System.Console.WriteLine("Attribute BooleanAttr3 Value {0}", attr2.BooleanValue);
        /// </code>
        /// </example>
        public GetRowResponse GetRow(GetRowRequest request)
        {
            return GetResponseFromAsyncTask(GetRowAsync(request));
        }

        /// <summary>
        /// GetRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<GetRowResponse> GetRowAsync(GetRowRequest request)
        {
            return CallAsync<GetRowRequest, GetRowResponse>("/GetRow", request);
        }

        /// <summary>
        /// 插入数据到指定的行，如果该行不存在，则新增一行；若该行存在，则覆盖原有行。
        /// 指定表名、主键和属性，写入一行数据。返回本次操作消耗的CapacityUnit。
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        /// <example>
        /// 写入1行数据，并打印出返回的读写能力消耗。
        /// <code>
        /// var primaryKey = new PrimaryKey();
        /// primaryKey.Add("PK0", new ColumnValue("ABC"));
        /// primaryKey.Add("PK1", new ColumnValue(123));
        /// 
        /// var attribute = new AttributeColumns();
        /// attribute.Add("IntAttr0", new ColumnValue(12345));
        /// attribute.Add("StringAttr1", new ColumnValue("ABC"));
        /// attribute.Add("DoubleAttr2", new ColumnValue(3.14));
        /// attribute.Add("BooleanAttr3", new ColumnValue(true));
        /// 
        /// var putRowRequest = new PutRowRequest(
        ///     "SampleTableName",
        ///     new Condition(RowExistenceExpectation.IGNORE),
        ///     primaryKey,
        ///     attribute
        /// );
        /// 
        /// var putRowResponse = otsClient.PutRow(putRowRequest);
        /// System.Console.WriteLine("PutRow CU Consumed: Read {0} Write {0}",
        ///                          putRowResponse.ConsumedCapacityUnit.Read,
        ///                          putRowResponse.ConsumedCapacityUnit.Write);
        /// </code>
        /// </example>
        public PutRowResponse PutRow(PutRowRequest request)
        {
            return GetResponseFromAsyncTask(PutRowAsync(request));
        }

        /// <summary>
        /// PutRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<PutRowResponse> PutRowAsync(PutRowRequest request)
        {
            return CallAsync<PutRowRequest, PutRowResponse>("/PutRow", request);
        }

        /// <summary>
        /// 更新指定行的数据，如果该行不存在，则新增一行；若该行存在，则根据请求的内容在这一行中新增、修改或者删除指定列的值。
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        /// <example>
        /// 更新一行，新增一列"NewColumn"，并删除一列"IntAttr0"。
        /// <code>
        /// var primaryKey = new PrimaryKey();
        /// primaryKey.Add("PK0", new ColumnValue("ABC"));
        /// primaryKey.Add("PK1", new ColumnValue(123));
        /// var updateOfAttribute = new UpdateOfAttribute();
        /// updateOfAttribute.AddAttributeColumnToPut("NewColumn", new ColumnValue(123));
        /// updateOfAttribute.AddAttributeColumnToDelete("IntAttr0");
        /// 
        /// var updateRowRequest = new UpdateRowRequest(
        ///                            "SampleTableName",
        ///                            new Condition(RowExistenceExpectation.EXPECT_EXIST),
        ///                            primaryKey,
        ///                            updateOfAttribute);
        /// var updateRowResponse = otsClient.UpdateRow(updateRowRequest);
        /// 
        /// System.Console.WriteLine("UpdateRow CU Consumed: Read {0} Write {0}",
        ///     updateRowResponse.ConsumedCapacityUnit.Read,
        ///     updateRowResponse.ConsumedCapacityUnit.Write);
        /// </code>
        /// </example>
        public UpdateRowResponse UpdateRow(UpdateRowRequest request)
        {
            return GetResponseFromAsyncTask(UpdateRowAsync(request));
        }

        /// <summary>
        /// UpdateRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<UpdateRowResponse> UpdateRowAsync(UpdateRowRequest request)
        {
            return CallAsync<UpdateRowRequest, UpdateRowResponse>("/UpdateRow", request);
        }

        /// <summary>
        /// 指定表名和主键，删除一行数据。
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        /// <example>
        /// 删除一行
        /// <code>
        /// var primaryKey = new PrimaryKey();
        /// primaryKey.Add("PK0", new ColumnValue("ABC"));
        /// primaryKey.Add("PK1", new ColumnValue(123));
        /// 
        /// var deleteRowRequest = new DeleteRowRequest(
        ///         "SampleTableName",
        ///         Condition.EXPECT_EXIST,
        ///         primaryKey);
        /// 
        /// var deleteRowResponse = otsClient.DeleteRow(deleteRowRequest);
        /// System.Console.WriteLine("DeleteRow CU Consumed: Read {0} Write {0}",
        ///         deleteRowResponse.ConsumedCapacityUnit.Read,
        ///         deleteRowResponse.ConsumedCapacityUnit.Write);
        /// </code>
        /// </example>
        public DeleteRowResponse DeleteRow(DeleteRowRequest request)
        {
            return GetResponseFromAsyncTask(DeleteRowAsync(request));
        }

        /// <summary>
        /// DeleteRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        /// 
        public Task<DeleteRowResponse> DeleteRowAsync(DeleteRowRequest request)
        {
            return CallAsync<DeleteRowRequest, DeleteRowResponse>("/DeleteRow", request);
        }

        #endregion

        #region Multiple Row Operations
        /// <summary>
        /// <para>批量读取一个或多个表中的若干行数据。</para>
        /// <para>BatchGetRow操作可视为多个GetRow操作的集合，各个操作独立执行，
        /// 独立返回结果，独立计算服务能力单元。</para>
        /// 与执行大量的GetRow操作相比，使用BatchGetRow操作可以有效减少请求的响应时间，提高数据的读取速率。
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        /// <example>
        /// 用BatchWriteRow读取2行数据
        /// <code>
        /// var primaryKey1 = new PrimaryKey();
        /// primaryKey1.Add("PK0", new ColumnValue("TestData"));
        /// primaryKey1.Add("PK1", new ColumnValue(0));
        /// 
        /// var primaryKey2 = new PrimaryKey();
        /// primaryKey2.Add("PK0", new ColumnValue("TestData"));
        /// primaryKey2.Add("PK1", new ColumnValue(1));
        /// 
        /// var batchGetRowRequest = new BatchGetRowRequest();
        /// var primaryKeys = new List&lt;PrimaryKey&gt;() {
        ///     primaryKey1,
        ///     primaryKey2
        /// };
        /// batchGetRowRequest.Add("SampleTableName", primaryKeys);
        /// var batchGetRowRespnse = otsClient.BatchGetRow(batchGetRowRequest);
        /// 
        /// foreach (var responseForOneTable in batchGetRowRespnse.RowDataGroupByTable) {
        ///     foreach (var row in responseForOneTable.Value) {
        ///         // 处理每一行的返回
        ///     }
        /// }
        /// </code>
        /// </example>
        public BatchGetRowResponse BatchGetRow(BatchGetRowRequest request)
        {
            return GetResponseFromAsyncTask(BatchGetRowAsync(request));
        }

        /// <summary>
        /// BatchGetRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<BatchGetRowResponse> BatchGetRowAsync(BatchGetRowRequest request)
        {
            return CallAsync<BatchGetRowRequest, BatchGetRowResponse>("/BatchGetRow", request);
        }

        /// <summary>
        /// <para>批量插入，修改或删除一个或多个表中的若干行数据。</para>
        /// <para>BatchWriteRow操作可视为多个PutRow、UpdateRow、DeleteRow操作的集合，各个操作独立执行，独立返回结果，独立计算服务能力单元。</para>
        /// <para>与执行大量的单行写操作相比，使用BatchWriteRow操作可以有效减少请求的响应时间，提高数据的写入速率。</para>
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        /// <example>
        /// 用BatchWriteRow写入2行并解析返回
        /// <code>
        /// var primaryKey1 = new PrimaryKey();
        /// primaryKey1.Add("PK0", new ColumnValue("TestData"));
        /// primaryKey1.Add("PK1", new ColumnValue(0));
        /// 
        /// var attribute1 = new AttributeColumns();
        /// attribute1.Add("Col0", new ColumnValue("Hangzhou"));
        /// 
        /// var primaryKey2 = new PrimaryKey();
        /// primaryKey2.Add("PK0", new ColumnValue("TestData"));
        /// primaryKey2.Add("PK1", new ColumnValue(1));
        /// 
        /// var attribute2 = new AttributeColumns();
        /// attribute2.Add("Col0", new ColumnValue("Shanghai"));
        /// 
        /// var rowChanges = new RowChanges();
        /// rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey1, attribute1);
        /// rowChanges.AddPut(new Condition(RowExistenceExpectation.IGNORE), primaryKey2, attribute2);
        /// var batchWriteRowRequest = new BatchWriteRowRequest();
        /// batchWriteRowRequest.Add("SampleTableName", rowChanges);
        /// 
        /// var batchWriteRowResponse = otsClient.BatchWriteRow(batchWriteRowRequest);
        /// foreach (var responseForOneTable in batchWriteRowResponse.TableRespones) {
        ///     foreach (var row in responseForOneTable.Value.PutResponses) {
        ///         // 处理每一行的返回
        ///     }
        /// }
        /// </code>
        /// </example>
        public BatchWriteRowResponse BatchWriteRow(BatchWriteRowRequest request)
        {
            return GetResponseFromAsyncTask(BatchWriteRowAsync(request));
        }

        /// <summary>
        /// BatchWriteRow的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<BatchWriteRowResponse> BatchWriteRowAsync(BatchWriteRowRequest request)
        {
            return CallAsync<BatchWriteRowRequest, BatchWriteRowResponse>("/BatchWriteRow", request);
        }

        /// <summary>
        /// 根据范围条件获取多行数据，返回用来迭代每一行数据的迭代器。
        /// </summary>
        /// <param name="request"><see cref="GetIteratorRequest"/></param>
        /// <returns>返回<see cref="Row"/>的迭代器。</returns>
        public IEnumerable<Row> GetRangeIterator(GetIteratorRequest request)
        {
            int? leftCount = request.QueryCriteria.Limit;

            if (leftCount != null && leftCount < 0)
            {
                throw new OTSClientException("the value of count must be larger than 0");
            }

            var nextStartPrimaryKey = request.QueryCriteria.InclusiveStartPrimaryKey;

            while (nextStartPrimaryKey != null)
            {
                request.QueryCriteria.InclusiveStartPrimaryKey = nextStartPrimaryKey;
                request.QueryCriteria.Limit = leftCount;

                var response = GetRange(request);
                request.ConsumedCapacityUnitCounter.Read += response.ConsumedCapacityUnit.Read;
                nextStartPrimaryKey = response.NextPrimaryKey;

                foreach (var rowData in response.RowDataList)
                {
                    yield return rowData;
                }

                if (leftCount != null)
                {
                    leftCount -= response.RowDataList.Count;
                    if (leftCount <= 0)
                    {
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// 根据范围条件获取多行数据，返回用来迭代每一行数据的迭代器。
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="direction">范围的方向，可以是<see cref="GetRangeDirection.Forward"/>或者<see cref="GetRangeDirection.Backward"/></param>
        /// <param name="inclusiveStartPrimaryKey">范围的开始主键，包含。</param>
        /// <param name="exclusiveEndPrimaryKey">范围的结束主键，不包含。</param>
        /// <param name="consumedCapacityUnitCounter">用户传入的CapacityUnit消耗计数器。</param>
        /// <param name="columnsToGet">可选参数，表示要获取的列的名称列表；默认获取所有列。</param>
        /// <param name="count">可选参数，表示最多获取多少行；默认获取范围内的所有行。</param>
        /// <returns>返回<see cref="Row"/>的迭代器。</returns>
        /// <example>
        /// <code>
        /// var startPrimaryKey = new PrimaryKey();
        /// startPrimaryKey.Add("PK0", new ColumnValue("TestData"));
        /// startPrimaryKey.Add("PK1", ColumnValue.INF_MIN);
        /// 
        /// var endPrimaryKey = new PrimaryKey();
        /// endPrimaryKey.Add("PK0", new ColumnValue("TestData"));
        /// endPrimaryKey.Add("PK1", ColumnValue.INF_MAX);
        /// 
        /// var consumed = new CapacityUnit(0, 0);
        /// 
        /// var iterator = otsClient.GetRangeIterator(
        ///         "SampleTableName",
        ///         GetRangeDirection.Forward,
        ///         startPrimaryKey,
        ///         endPrimaryKey,
        ///         consumed);
        /// 
        /// foreach (var rowData in iterator) {
        ///     // 处理每一行数据
        /// }
        /// </code>
        /// </example>
        [Obsolete("GetRangeIterator(string ...) is deprecated, please use GetRangeIterator(GetIteratorRequest request) instead.")]
        public IEnumerable<Row> GetRangeIterator(
            string tableName,
            GetRangeDirection direction,
            PrimaryKey inclusiveStartPrimaryKey,
            PrimaryKey exclusiveEndPrimaryKey,
            CapacityUnit consumedCapacityUnitCounter,
            HashSet<string> columnsToGet = null,
            int? count = null,
            IColumnCondition condition = null)
        {
            int? leftCount = count;

            if (leftCount != null && leftCount < 0)
            {
                throw new OTSClientException("the value of count must be larger than 0");
            }

            PrimaryKey nextStartPrimaryKey = inclusiveStartPrimaryKey;

            while (nextStartPrimaryKey != null)
            {
                var request = new GetRangeRequest(
                    tableName, direction, nextStartPrimaryKey, exclusiveEndPrimaryKey,
                    columnsToGet, leftCount, condition);

                var response = GetRange(request);
                consumedCapacityUnitCounter.Read += response.ConsumedCapacityUnit.Read;
                nextStartPrimaryKey = response.NextPrimaryKey;

                foreach (var rowData in response.RowDataList)
                {
                    yield return rowData;
                }

                if (leftCount != null)
                {
                    leftCount -= response.RowDataList.Count;
                    if (leftCount <= 0)
                    {
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// 根据范围条件获取多行数据。
        /// </summary>
        /// <param name="request">请求实例</param>
        /// <returns>响应实例</returns>
        public GetRangeResponse GetRange(GetRangeRequest request)
        {
            return GetResponseFromAsyncTask(GetRangeAsync(request));
        }

        /// <summary>
        /// GetRange的异步版本。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<GetRangeResponse> GetRangeAsync(GetRangeRequest request)
        {
            return CallAsync<GetRangeRequest, GetRangeResponse>("/GetRange", request);
        }

        #endregion

        #region Search Operations
        public ListSearchIndexResponse ListSearchIndex(ListSearchIndexRequest request)
        {
            return GetResponseFromAsyncTask(ListSearchIndexAsync(request));
        }

        /// <summary>
        /// ListSearchIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<ListSearchIndexResponse> ListSearchIndexAsync(ListSearchIndexRequest request)
        {
            return CallAsync<ListSearchIndexRequest, ListSearchIndexResponse>("/ListSearchIndex", request);
        }

        public CreateSearchIndexResponse CreateSearchIndex(CreateSearchIndexRequest request)
        {
            return GetResponseFromAsyncTask(CreateSearchIndexAsync(request));
        }

        /// <summary>
        /// CreateSearchIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<CreateSearchIndexResponse> CreateSearchIndexAsync(CreateSearchIndexRequest request)
        {
            return CallAsync<CreateSearchIndexRequest, CreateSearchIndexResponse>("/CreateSearchIndex", request);
        }

        public DeleteSearchIndexResponse DeleteSearchIndex(DeleteSearchIndexRequest request)
        {
            return GetResponseFromAsyncTask(DeleteSearchIndexAsync(request));
        }

        /// <summary>
        /// DeleteSearchIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<DeleteSearchIndexResponse> DeleteSearchIndexAsync(DeleteSearchIndexRequest request)
        {
            return CallAsync<DeleteSearchIndexRequest, DeleteSearchIndexResponse>("/DeleteSearchIndex", request);
        }

        public DescribeSearchIndexResponse DescribeSearchIndex(DescribeSearchIndexRequest request)
        {
            return GetResponseFromAsyncTask(DescribeSearchIndexAsync(request));
        }

        /// <summary>
        /// DescribeSearchIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<DescribeSearchIndexResponse> DescribeSearchIndexAsync(DescribeSearchIndexRequest request)
        {
            return CallAsync<DescribeSearchIndexRequest, DescribeSearchIndexResponse>("/DescribeSearchIndex", request);
        }

        public SearchResponse Search(SearchRequest request)
        {
            return GetResponseFromAsyncTask(SearchAsync(request));
        }

        /// <summary>
        /// Search的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<SearchResponse> SearchAsync(SearchRequest request)
        {
            return CallAsync<SearchRequest, SearchResponse>("/Search", request);
        }
        #endregion


        #region Search Operations


        public CreateGlobalIndexResponse CreateGlobalIndex(CreateGlobalIndexRequest request)
        {
            return GetResponseFromAsyncTask(CreateGlobalIndexAsync(request));
        }

        /// <summary>
        /// CreateGlobalIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<CreateGlobalIndexResponse> CreateGlobalIndexAsync(CreateGlobalIndexRequest request)
        {
            return CallAsync<CreateGlobalIndexRequest, CreateGlobalIndexResponse>("/CreateIndex", request);
        }

        public DeleteGlobalIndexResponse DeleteGlobalIndex(DeleteGlobalIndexRequest request)
        {
            return GetResponseFromAsyncTask(DeleteGlobalIndexAsync(request));
        }

        /// <summary>
        /// CreateGlobalIndex的异步形式。
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public Task<DeleteGlobalIndexResponse> DeleteGlobalIndexAsync(DeleteGlobalIndexRequest request)
        {
            return CallAsync<DeleteGlobalIndexRequest, DeleteGlobalIndexResponse>("/DropIndex", request);
        }

        #endregion


        #region Private Function

        private void ThrowIfNullRequest<TRequestType>(TRequestType request)
        {
            if (request == null)
            {
                throw new ArgumentNullException("request");
            }
        }


        private Task<TResponse> CallAsync<TRequest, TResponse>(string apiName, TRequest request)
            where TResponse : OTSResponse, new()
            where TRequest : OTSRequest
        {
            ThrowIfNullRequest(request);

            Context otsContext = new Context
            {
                ClientConfig = ClientConfig,
                APIName = apiName,
                OTSRequest = request,
                OTSReponse = new TResponse(),
                HttpClient = client
            };

            OTSHandler.HandleBefore(otsContext);

            return otsContext.HttpTask.ContinueWith((t) =>
            {
                // ConnectionPool.ReturnHttpClient(otsContext.HttpClient);
                OTSHandler.HandleAfter(otsContext);
                return (TResponse)otsContext.OTSReponse;
            });
        }

        private TResponse GetResponseFromAsyncTask<TResponse>(Task<TResponse> task)
        {
            try
            {
                task.Wait();
            }
            catch (AggregateException e)
            {
                if (ClientConfig.OTSErrorLogHandler != null)
                {
                    ClientConfig.OTSErrorLogHandler.Invoke("Exception:\n" + e.GetBaseException().StackTrace + "\n");
                }

                throw e.GetBaseException();
            }

            return task.Result;
        }

        #endregion
    }
}
