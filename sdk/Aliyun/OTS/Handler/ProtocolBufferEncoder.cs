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

using System;
using System.Collections.Generic;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;
using Aliyun.OTS.DataModel.Filter;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Handler
{
    public class ProtocolBufferEncoder : PipelineHandler
    {
        private delegate IMessage RequestEncoder(Request.OTSRequest request);
        private readonly Dictionary<string, RequestEncoder> EncoderMap;

        public ProtocolBufferEncoder(PipelineHandler innerHandler) : base(innerHandler)
        {
            EncoderMap = new Dictionary<string, RequestEncoder> {
                { "/CreateTable",          EncodeCreateTable },
                { "/DeleteTable",          EncodeDeleteTable },
                { "/UpdateTable",          EncodeUpdateTable },
                { "/DescribeTable",        EncodeDescribeTable },
                { "/ListTable",            EncodeListTable },

                { "/PutRow",               EncodePutRow },
                { "/GetRow",               EncodeGetRow },
                { "/UpdateRow",            EncodeUpdateRow },
                { "/DeleteRow",            EncodeDeleteRow },

                { "/BatchWriteRow",        EncodeBatchWriteRow },
                { "/BatchGetRow",          EncodeBatchGetRow },
                { "/GetRange",             EncodeGetRange },
            };
        }

        public override void HandleBefore(Context context)
        {
            var encoder = EncoderMap[context.APIName];
            var message = encoder(context.OTSRequest);
            LogEncodedMessage(context, message);
            context.HttpRequestBody = message.ToByteArray();
            InnerHandler.HandleBefore(context);
        }

        public override void HandleAfter(Context context)
        {
            InnerHandler.HandleAfter(context);
        }

        #region Encode Request
        private IMessage EncodeCreateTable(Request.OTSRequest request)
        {
            var requestReal = (Request.CreateTableRequest)request;
            var builder = PB.CreateTableRequest.CreateBuilder();
            builder.SetTableMeta(EncodeTableMeta(requestReal.TableMeta));
            builder.SetReservedThroughput(EncodeReservedThroughput(requestReal.ReservedThroughput));

            builder.SetTableOptions(EncodeTableOptions(requestReal.TableOptions));

            if (requestReal.StreamSpecification != null)
            {
                builder.SetStreamSpec(EncodeStreamSpecification(requestReal.StreamSpecification));
            }

            if (requestReal.PartitionRange != null)
            {
                for (var i = 0; i < requestReal.PartitionRange.Count; i++)
                {
                    builder.SetPartitions(i, EncodePartitionRange(requestReal.PartitionRange[i]));
                }
            }

            var message = builder.Build();
            return message;
        }

        private IMessage EncodeDeleteTable(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteTableRequest)request;
            var builder = PB.DeleteTableRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            return builder.Build();
        }

        private IMessage EncodeUpdateTable(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateTableRequest)request;
            var builder = PB.UpdateTableRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);

            if (requestReal.ReservedThroughput != null)
            {
                builder.SetReservedThroughput(EncodeReservedThroughput(requestReal.ReservedThroughput));
            }

            if (requestReal.TableOptions != null)
            {
                builder.SetTableOptions(EncodeTableOptions(requestReal.TableOptions));
            }

            if (requestReal.StreamSpecification != null)
            {
                builder.SetStreamSpec(EncodeStreamSpecification(requestReal.StreamSpecification));
            }

            return builder.Build();
        }

        private IMessage EncodeDescribeTable(Request.OTSRequest request)
        {
            var requestReal = (Request.DescribeTableRequest)request;
            var builder = PB.DescribeTableRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            return builder.Build();
        }

        private IMessage EncodeListTable(Request.OTSRequest request)
        {
            var builder = PB.ListTableRequest.CreateBuilder();
            return builder.Build();
        }

        private IMessage EncodePutRow(Request.OTSRequest request)
        {
            var requestReal = (Request.PutRowRequest)request;
            var builder = PB.PutRowRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetCondition(EncodeCondition(requestReal.Condition));
            builder.SetRow(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowPutChangeWithHeader(requestReal.RowPutChange)));
            builder.SetReturnContent(EncodeReturnContent(requestReal.RowPutChange.ReturnType));
            return builder.Build();
        }

        private IMessage EncodeGetRow(Request.OTSRequest request)
        {
            var requestReal = (Request.GetRowRequest)request;
            var queryCriteria = requestReal.QueryCriteria;

            var builder = PB.GetRowRequest.CreateBuilder();
            builder.SetTableName(queryCriteria.TableName);
            builder.SetPrimaryKey(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyWithHeader(queryCriteria.RowPrimaryKey)));

            builder.AddRangeColumnsToGet(queryCriteria.GetColumnsToGet());

            // if timeRange and maxVersions are both not set, set maxVersions to 1 default
            if (!CheckQueryCondition(queryCriteria.TableName, queryCriteria.MaxVersions, queryCriteria.TimeRange))
            {
                queryCriteria.MaxVersions = 1;
            }

            if (queryCriteria.TimeRange != null)
            {
                builder.SetTimeRange(EncodeTimeRange(queryCriteria.TimeRange));
            }

            if (queryCriteria.MaxVersions.HasValue)
            {
                builder.SetMaxVersions(queryCriteria.MaxVersions.Value);
            }

            if (queryCriteria.CacheBlocks != null && queryCriteria.CacheBlocks.HasValue)
            {
                builder.SetCacheBlocks(queryCriteria.CacheBlocks.Value);
            }

            if (queryCriteria.Filter != null)
            {
                builder.SetFilter(BuildFilter(queryCriteria.Filter));
            }

            if (queryCriteria.StartColumn != null)
            {
                builder.SetStartColumn(queryCriteria.StartColumn);
            }

            if (queryCriteria.EndColumn != null)
            {
                builder.SetEndColumn(queryCriteria.EndColumn);
            }

            if (queryCriteria.Token != null)
            {
                builder.SetToken(ByteString.CopyFrom(queryCriteria.Token));
            }


            return builder.Build();
        }

        private IMessage EncodeUpdateRow(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateRowRequest)request;
            var builder = PB.UpdateRowRequest.CreateBuilder();

            var rowChange = requestReal.RowUpdateChange;

            // required string table_name = 1;
            builder.SetTableName(rowChange.TableName);
            // required bytes row_change = 2;
            builder.SetRowChange(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowUpdateChangeWithHeader(rowChange)));
            // required Condition condition = 3;
            builder.SetCondition(EncodeCondition(rowChange.Condition));
            // option ReturnType = 4;
            builder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType));

            return builder.Build();
        }

        private IMessage EncodeDeleteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteRowRequest)request;
            var rowChange = requestReal.RowDeleteChange;
            var builder = PB.DeleteRowRequest.CreateBuilder();

            // required string table_name = 1;
            builder.SetTableName(rowChange.TableName);
            // required bytes primary_key = 2;
            builder.SetPrimaryKey(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowDeleteChangeWithHeader(rowChange)));
            // required Condition condition = 3;
            builder.SetCondition(EncodeCondition(rowChange.Condition));
            // option ReturnType = 4;
            builder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType));
            return builder.Build();
        }

        private IMessage EncodeBatchWriteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchWriteRowRequest)request;
            var builder = PB.BatchWriteRowRequest.CreateBuilder();

            foreach (var item in requestReal.RowChangesGroupByTable)
            {
                builder.AddTables(EncodeTableInBatchWriteRowRequest(item.Key, item.Value));
            }

            return builder.Build();
        }

        private IMessage EncodeBatchGetRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchGetRowRequest)request;
            var builder = PB.BatchGetRowRequest.CreateBuilder();

            foreach (var criterias in requestReal.GetCriterias())
            {
                builder.AddTables(EncodeTableInBatchGetRowRequest(criterias));
            }

            return builder.Build();
        }

        private IMessage EncodeGetRange(Request.OTSRequest request)
        {
            var requestReal = (Request.GetRangeRequest)request;
            var builder = PB.GetRangeRequest.CreateBuilder();

            var queryCriteria = requestReal.QueryCriteria;

            // required string table_name = 1;
            builder.SetTableName(queryCriteria.TableName);

            // required Direction direction = 2;
            builder.SetDirection(ToPBDirection(queryCriteria.Direction));

            // repeated string columns_to_get = 3;
            if (queryCriteria.GetColumnsToGet() != null)
            {
                builder.AddRangeColumnsToGet(queryCriteria.GetColumnsToGet());
            }

            // if timeRange and maxVersions are both not set, set maxVersions to 1 default
            if (!CheckQueryCondition(queryCriteria.TableName, queryCriteria.MaxVersions, queryCriteria.TimeRange))
            {
                queryCriteria.MaxVersions = 1;
            }

            // optional TimeRange time_range = 4;
            if (queryCriteria.TimeRange != null)
            {
                builder.SetTimeRange(EncodeTimeRange(queryCriteria.TimeRange));
            }

            // optional int32 max_versions = 5;
            if (queryCriteria.MaxVersions.HasValue)
            {
                builder.SetMaxVersions(queryCriteria.MaxVersions.Value);
            }

            // optional int32 limit = 6;
            if (queryCriteria.Limit != null && queryCriteria.Limit > 0)
            {
                builder.SetLimit((int)queryCriteria.Limit);
            }

            // required bytes inclusive_start_primary_key = 7;
            builder.SetInclusiveStartPrimaryKey(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyWithHeader(queryCriteria.InclusiveStartPrimaryKey)));
            // required bytes exclusive_end_primary_key = 8;
            builder.SetExclusiveEndPrimaryKey(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyWithHeader(queryCriteria.ExclusiveEndPrimaryKey)));

            // optional bool cache_blocks = 9 [default = true];
            if (queryCriteria.CacheBlocks.HasValue)
            {
                builder.SetCacheBlocks(queryCriteria.CacheBlocks.Value);
            }

            // optional bytes filter = 10;
            if (queryCriteria.Filter != null)
            {
                builder.SetFilter(BuildFilter(queryCriteria.Filter));
            }

            // optional string start_column = 11;
            if (queryCriteria.StartColumn != null)
            {
                builder.SetStartColumn(queryCriteria.StartColumn);
            }

            // optional string end_column = 12;
            if (queryCriteria.EndColumn != null)
            {
                builder.SetEndColumn(queryCriteria.EndColumn);
            }

            // optional bytes token = 13;
            if (queryCriteria.HasSetToken())
            {
                builder.SetToken(ByteString.CopyFrom(queryCriteria.Token));
            }

            return builder.Build();
        }

        #endregion

        #region Encode Others
        private PB.TableOptions EncodeTableOptions(DataModel.TableOptions tableOptions)
        {
            var builder = PB.TableOptions.CreateBuilder();

            if (tableOptions.TimeToLive.HasValue)
            {
                builder.SetTimeToLive(tableOptions.TimeToLive.Value);
            }
            else
            {
                Console.WriteLine("Time to live is set to -1 in default.");
                builder.SetTimeToLive(-1);
            }

            if (tableOptions.MaxVersions.HasValue)
            {
                builder.SetMaxVersions(tableOptions.MaxVersions.Value);
            }
            else
            {
                Console.WriteLine("Max Versions is set to 1 in default.");
                builder.SetMaxVersions(1);
            }

            if (tableOptions.DeviationCellVersionInSec.HasValue)
            {
                builder.SetDeviationCellVersionInSec(tableOptions.DeviationCellVersionInSec.Value);
            }

            if (tableOptions.BlockSize.HasValue)
            {
                builder.SetBlockSize(tableOptions.BlockSize.Value);
            }

            if(tableOptions.BloomFilterType.HasValue)
            {
                builder.SetBloomFilterType(EncodeBloomFilterType(tableOptions.BloomFilterType.Value));
            }


            return builder.Build();
        }

        private PB.BloomFilterType EncodeBloomFilterType(DataModel.BloomFilterType bloomFilterType)
        {

            switch (bloomFilterType)
            {
                case DataModel.BloomFilterType.CELL:
                    return PB.BloomFilterType.CELL;
                case DataModel.BloomFilterType.NONE:
                    return PB.BloomFilterType.NONE;
                case DataModel.BloomFilterType.ROW:
                    return PB.BloomFilterType.ROW;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid bloomFilterType {0}", bloomFilterType)
                    );
            }
        }

        private PB.StreamSpecification EncodeStreamSpecification(DataModel.StreamSpecification streamSpecification)
        {
            var builder = new PB.StreamSpecification.Builder();
            builder.SetEnableStream(streamSpecification.EnableStream);
            builder.SetExpirationTime(streamSpecification.ExpirationTime);
            return builder.Build();
        }

        private PB.PartitionRange EncodePartitionRange(DataModel.PartitionRange partitionRange)
        {
            var builder = PB.PartitionRange.CreateBuilder();
            builder.SetBegin(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyValueWithoutLengthPrefix(partitionRange.Begin)));
            builder.SetEnd(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyValueWithoutLengthPrefix(partitionRange.End)));
            return builder.Build();
        }

        private PB.TableMeta EncodeTableMeta(DataModel.TableMeta tableMeta)
        {
            var builder = PB.TableMeta.CreateBuilder();
            builder.SetTableName(tableMeta.TableName);
            builder.AddRangePrimaryKey(EncodePrimaryKeySchema(tableMeta.PrimaryKeySchema));
            return builder.Build();
        }

        private IEnumerable<PB.PrimaryKeySchema> EncodePrimaryKeySchema(DataModel.PrimaryKeySchema schema)
        {
            foreach (var item in schema)
            {
                yield return EncodeColumnSchema(item);
            }
        }

        private PB.PrimaryKeySchema EncodeColumnSchema(Tuple<string, DataModel.ColumnValueType, DataModel.PrimaryKeyOption> schema)
        {
            var builder = PB.PrimaryKeySchema.CreateBuilder();
            builder.SetName(schema.Item1);
            builder.SetType(EncodeColumnType(schema.Item2));

            if (schema.Item3 != DataModel.PrimaryKeyOption.NONE)
            {
                builder.SetOption(EncodePrimaryKeyOption(schema.Item3));
            }

            return builder.Build();
        }

        private PB.PrimaryKeyType EncodeColumnType(DataModel.ColumnValueType type)
        {
            switch (type)
            {
                case DataModel.ColumnValueType.Integer:
                    return PB.PrimaryKeyType.INTEGER;
                case DataModel.ColumnValueType.String:
                    return PB.PrimaryKeyType.STRING;
                case DataModel.ColumnValueType.Binary:
                    return PB.PrimaryKeyType.BINARY;
                default:
                    throw new OTSClientException(String.Format(
                        "Invalid column value type: {0}", type
                    ));
            }
        }

        private PB.PrimaryKeyOption EncodePrimaryKeyOption(DataModel.PrimaryKeyOption option)
        {
            switch(option)
            {
                case DataModel.PrimaryKeyOption.AUTO_INCREMENT:
                    return PB.PrimaryKeyOption.AUTO_INCREMENT;
                default:
                    throw new OTSClientException(String.Format(
                        "Invalid primary key option: {0}", option
                    ));
            }
        }

        private PB.ReservedThroughput EncodeReservedThroughput(DataModel.CapacityUnit capacityUnit)
        {
            var builder = PB.ReservedThroughput.CreateBuilder();
            builder.SetCapacityUnit(EncodeCapacityUnit(capacityUnit));
            return builder.Build();
        }

        private PB.CapacityUnit EncodeCapacityUnit(DataModel.CapacityUnit capacityUnit)
        {
            var builder = PB.CapacityUnit.CreateBuilder();

            if (capacityUnit.Read.HasValue)
            {
                builder.SetRead(capacityUnit.Read.Value);
            }

            if (capacityUnit.Write.HasValue)
            {
                builder.SetWrite(capacityUnit.Write.Value);
            }

            return builder.Build();
        }

        private PB.Condition EncodeCondition(DataModel.Condition condition)
        {
            PB.Condition.Builder builder = PB.Condition.CreateBuilder();
            switch (condition.RowExistenceExpect)
            {
                case DataModel.RowExistenceExpectation.EXPECT_EXIST:
                    builder.SetRowExistence(PB.RowExistenceExpectation.EXPECT_EXIST);
                    break;
                case DataModel.RowExistenceExpectation.EXPECT_NOT_EXIST:
                    builder.SetRowExistence(PB.RowExistenceExpectation.EXPECT_NOT_EXIST);
                    break;
                case DataModel.RowExistenceExpectation.IGNORE:
                    builder.SetRowExistence(PB.RowExistenceExpectation.IGNORE);
                    break;
                default:
                    throw new OTSClientException(String.Format("Invalid RowExistenceExpectation: {0}", condition.RowExistenceExpect));
            }

            if (condition.ColumnCondition != null)
            {
                builder.SetColumnCondition(BuildFilter(condition.ColumnCondition));
            }

            return builder.Build();
        }

        private static ByteString BuildFilter(IColumnCondition filter)
        {
            PB.Filter.Builder builder = PB.Filter.CreateBuilder();

            builder.SetType(EncodeFilterType(filter.GetConditionType()));
            builder.SetFilter_(filter.Serialize());
            return builder.Build().ToByteString();
        }

        private static ByteString BuildFilter(IFilter filter)
        {
            PB.Filter.Builder builder = PB.Filter.CreateBuilder();

            builder.SetType(ToPBFilterType(filter.GetFilterType()));
            builder.SetFilter_(filter.Serialize());
            return builder.Build().ToByteString();
        }

        private static PB.FilterType EncodeFilterType(ColumnConditionType type)
        {
            switch (type)
            {
                case ColumnConditionType.COMPOSITE_CONDITION:
                    return PB.FilterType.FT_COMPOSITE_COLUMN_VALUE;
                case ColumnConditionType.RELATIONAL_CONDITION:
                    return PB.FilterType.FT_SINGLE_COLUMN_VALUE;
                default:
                    throw new ArgumentException("Unknown filter type: " + type);
            }
        }

        private static PB.ReturnContent EncodeReturnContent(DataModel.ReturnType returnType)
        {
            PB.ReturnContent.Builder builder = PB.ReturnContent.CreateBuilder();
            builder.SetReturnType(ToPBReturnType(returnType));

            return builder.Build();
        }

        private static PB.TimeRange EncodeTimeRange(DataModel.TimeRange timeRange)
        {
            PB.TimeRange.Builder builder = PB.TimeRange.CreateBuilder();
            if (timeRange.SpecificTime.HasValue)
            {
                builder.SetSpecificTime(timeRange.SpecificTime.Value);
            }
            else
            {
                builder.SetStartTime(timeRange.StartTime.Value);
                builder.SetEndTime(timeRange.EndTime.Value);
            }

            return builder.Build();
        }

        private PB.TableInBatchWriteRowRequest EncodeTableInBatchWriteRowRequest(string tableName, DataModel.RowChanges rowChanges)
        {
            var tableBuilder = PB.TableInBatchWriteRowRequest.CreateBuilder();

            tableBuilder.SetTableName(tableName);

            if (rowChanges == null || rowChanges.IsEmpty())
            {
                return tableBuilder.Build();
            }

            foreach (var rowChange in rowChanges.RowPutChanges)
            {
                tableBuilder.AddRows(EncodeWriteRowRequest(rowChange));
            }

            foreach (var rowChange in rowChanges.RowUpdateChanges)
            {
                tableBuilder.AddRows(EncodeWriteRowRequest(rowChange));
            }

            foreach (var rowChange in rowChanges.RowDeleteChanges)
            {
                tableBuilder.AddRows(EncodeWriteRowRequest(rowChange));
            }

            return tableBuilder.Build();
        }

        private PB.RowInBatchWriteRowRequest EncodeWriteRowRequest(DataModel.RowChange rowChange)
        {
            var rowBuilder = PB.RowInBatchWriteRowRequest.CreateBuilder();

            if (rowChange is DataModel.RowPutChange)
            {
                rowBuilder.SetType(PB.OperationType.PUT);
                rowBuilder.SetRowChange(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowPutChangeWithHeader(rowChange as DataModel.RowPutChange)));
            }
            else if (rowChange is DataModel.RowUpdateChange)
            {
                rowBuilder.SetType(PB.OperationType.UPDATE);
                rowBuilder.SetRowChange(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowUpdateChangeWithHeader(rowChange as DataModel.RowUpdateChange)));
            }
            else if (rowChange is DataModel.RowDeleteChange)
            {
                rowBuilder.SetType(PB.OperationType.DELETE);
                rowBuilder.SetRowChange(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildRowDeleteChangeWithHeader(rowChange as DataModel.RowDeleteChange)));
            }
            else
            {
                throw new OTSException("unkown row change " + rowChange.GetType());
            }

            rowBuilder.SetCondition(EncodeCondition(rowChange.Condition));
            rowBuilder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType));
            return rowBuilder.Build();
        }

        private PB.TableInBatchGetRowRequest EncodeTableInBatchGetRowRequest(DataModel.MultiRowQueryCriteria criteria)
        {
            var tableBuilder = PB.TableInBatchGetRowRequest.CreateBuilder();
            tableBuilder.SetTableName(criteria.TableName);


            if (criteria.GetRowKeys().Count != criteria.GetTokens().Count)
            {
                throw new OTSException("The number of primaryKeys and tokens must be the same. Table name:" + criteria.TableName);
            }

            // if timeRange and maxVersions are both not set, set maxVersions to 1 default
            if (!CheckQueryCondition(criteria.TableName, criteria.MaxVersions, criteria.TimeRange))
            {
                criteria.MaxVersions = 1;
            }

            // repeated bytes primary_key = 2;
            // repeated bytes tokens = 3;
            for (int i = 0; i < criteria.Size(); i++)
            {
                tableBuilder.AddPrimaryKey(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildPrimaryKeyWithHeader(criteria.Get(i))));
                tableBuilder.AddToken(ByteString.CopyFrom(criteria.GetTokens()[i]));
            }

            // repeated string columns_to_get = 4;
            if (criteria.GetColumnsToGet() != null)
            {
                tableBuilder.AddRangeColumnsToGet(criteria.GetColumnsToGet());
            }

            // optional TimeRange time_range = 5;
            if (criteria.TimeRange != null)
            {
                tableBuilder.SetTimeRange(EncodeTimeRange(criteria.TimeRange));
            }

            // optional int32 max_versions = 6;
            if (criteria.MaxVersions.HasValue)
            {
                tableBuilder.SetMaxVersions(criteria.MaxVersions.Value);
            }

            // optional bool cache_blocks = 7;
            if (criteria.CacheBlocks.HasValue)
            {
                tableBuilder.SetCacheBlocks(criteria.CacheBlocks.Value);
            }

            // optional bytes filter = 8;
            if (criteria.Filter != null)
            {
                tableBuilder.SetFilter(BuildFilter(criteria.Filter));
            }

            // optional string startColumn = 9;
            if (criteria.StartColumn != null)
            {
                tableBuilder.SetStartColumn(criteria.StartColumn);
            }

            // optional string endColumn = 10;
            if (criteria.EndColumn != null)
            {
                tableBuilder.SetEndColumn(criteria.EndColumn);
            }

            return tableBuilder.Build();
        }

        #endregion

        private void LogEncodedMessage(Context context, IMessage message)
        {
            if (context.ClientConfig.OTSDebugLogHandler != null)
            {
                var msgString = String.Format("OTS Request API: {0} Protobuf: {1}\n",
                                    context.APIName,
                                    TextFormat.PrintToString(message));
                context.ClientConfig.OTSDebugLogHandler(msgString);
            }
        }

        private static PB.ComparatorType ToPBComparatorType(DataModel.CompareOperator compareOperator)
        {
            switch (compareOperator)
            {
                case DataModel.CompareOperator.EQUAL:
                    return PB.ComparatorType.CT_EQUAL;
                case DataModel.CompareOperator.NOT_EQUAL:
                    return PB.ComparatorType.CT_NOT_EQUAL;
                case DataModel.CompareOperator.GREATER_THAN:
                    return PB.ComparatorType.CT_GREATER_THAN;
                case DataModel.CompareOperator.GREATER_EQUAL:
                    return PB.ComparatorType.CT_GREATER_EQUAL;
                case DataModel.CompareOperator.LESS_THAN:
                    return PB.ComparatorType.CT_LESS_THAN;
                case DataModel.CompareOperator.LESS_EQUAL:
                    return PB.ComparatorType.CT_LESS_EQUAL;
                default:
                    throw new ArgumentException("Unknown compare operator: " + compareOperator);
            }
        }

        private static PB.ReturnType ToPBReturnType(DataModel.ReturnType returnType)
        {
            switch (returnType)
            {
                case DataModel.ReturnType.RT_NONE:
                    return PB.ReturnType.RT_NONE;
                case DataModel.ReturnType.RT_PK:
                    return PB.ReturnType.RT_PK;
                default:
                    throw new ArgumentException("Invalid return type: " + returnType);
            }
        }

        private static PB.Direction ToPBDirection(Request.GetRangeDirection direction)
        {
            switch (direction)
            {
                case Request.GetRangeDirection.Forward:
                    return PB.Direction.FORWARD;
                case Request.GetRangeDirection.Backward:
                    return PB.Direction.BACKWARD;
                default:
                    throw new ArgumentException("unknown direction type:" + direction);
            }
        }

        private static PB.FilterType ToPBFilterType(FilterType filterType)
        {
            switch (filterType)
            {
                case FilterType.SINGLE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_SINGLE_COLUMN_VALUE;
                case FilterType.COMPOSITE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_COMPOSITE_COLUMN_VALUE;
                case FilterType.COLUMN_PAGINATION_FILTER:
                    return PB.FilterType.FT_COLUMN_PAGINATION;
                default:
                    throw new ArgumentException("Unknown filter type: " + filterType);
            }
        }

        private bool CheckQueryCondition(string tableName, int? maxVersions, DataModel.TimeRange timeRange)
        {
            if (maxVersions.HasValue && timeRange != null)
            {
                throw new OTSException("Error, MaxVersions and TimeRange can NOT be specified at the same time. Table name:" + tableName);
            }

            if (!maxVersions.HasValue && timeRange == null)
            {
                return false;
            }

            return true;
        }
    }
}
