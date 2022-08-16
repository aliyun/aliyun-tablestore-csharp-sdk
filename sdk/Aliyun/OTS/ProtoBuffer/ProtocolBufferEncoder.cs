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
using com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.Handler;

namespace Aliyun.OTS.ProtoBuffer
{
    public class ProtocolBufferEncoder : PipelineHandler
    {
        private delegate IMessage RequestEncoder(Request.OTSRequest request);
        private readonly Dictionary<string, RequestEncoder> EncoderMap;
        private readonly int DEFAULT_NUMBER_OF_SHARDS = 1;

        public ProtocolBufferEncoder(PipelineHandler innerHandler) : base(innerHandler)
        {
            EncoderMap = new Dictionary<string, RequestEncoder> {
                { "/CreateTable",            EncodeCreateTable },
                { "/DeleteTable",            EncodeDeleteTable },
                { "/UpdateTable",            EncodeUpdateTable },
                { "/DescribeTable",          EncodeDescribeTable },
                { "/ListTable",              EncodeListTable },

                { "/PutRow",                 EncodePutRow },
                { "/GetRow",                 EncodeGetRow },
                { "/UpdateRow",              EncodeUpdateRow },
                { "/DeleteRow",              EncodeDeleteRow },

                { "/BatchWriteRow",          EncodeBatchWriteRow },
                { "/BatchGetRow",            EncodeBatchGetRow },
                { "/GetRange",               EncodeGetRange },

                { "/ListSearchIndex",        EncodeListSearchIndex },
                { "/CreateSearchIndex",      EncodeCreateSearchIndex },
                { "/ComputeSplits",          EncodeComputeSplits},
                { "/DescribeSearchIndex",    EncodeDescribeSearchIndex },
                { "/DeleteSearchIndex",      EncodeDeleteSearchIndex },
                { "/ParallelScan",           EncodeParallelScan },
                { "/UpdateSearchIndex",      EncodeUpdateSearchIndex },
                { "/Search",                 EncodeSearch },

                { "/CreateIndex",            EncodeCreateGlobalIndex },
                { "/DropIndex",              EncodeDeleteGlobalIndex },

                { "/SQLQuery",               EncodeSQLQuery },
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

            if (requestReal.IndexMetas != null)
            {
                for (int i = 0; i < requestReal.IndexMetas.Count; i++)
                {
                    builder.AddIndexMetas(EncodeIndexMeta(requestReal.IndexMetas[i]));
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
            builder.SetReturnContent(EncodeReturnContent(requestReal.RowPutChange.ReturnType, requestReal.RowPutChange.ReturnColumnNames));
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
            builder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType, rowChange.ReturnColumnNames));

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
            builder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType, rowChange.ReturnColumnNames));
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
            if (queryCriteria.Limit.HasValue && queryCriteria.Limit > 0)
            {
                builder.SetLimit(queryCriteria.Limit.Value);
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


        private IMessage EncodeListSearchIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.ListSearchIndexRequest)request;
            var builder = PB.ListSearchIndexRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            return builder.Build();
        }

        private IMessage EncodeCreateSearchIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.CreateSearchIndexRequest)request;
            var builder = PB.CreateSearchIndexRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.IndexName);

            if (requestReal.IndexSchame != null)
            {
                builder.SetSchema(EncodeIndexSchema(requestReal.IndexSchame));
            }

            if (requestReal.SourceIndexName != null)
            {
                builder.SetSourceIndexName(requestReal.SourceIndexName);
            }

            if (requestReal.TimeToLive.HasValue)
            {
                builder.SetTimeToLive(requestReal.TimeToLive.Value);
            }

            return builder.Build();
        }

        private IMessage EncodeComputeSplits(Request.OTSRequest request)
        {
            var requestReal = (Request.ComputeSplitsRequest)request;
            var builder = PB.ComputeSplitsRequest.CreateBuilder();

            builder.SetTableName(requestReal.TableName);

            if (requestReal.SplitOptions != null)
            {
                builder.SetSearchIndexSplitsOptions(EncodeSearchIndexOptions(requestReal.SplitOptions));
            }

            return builder.Build();
        }

        private IMessage EncodeSearch(Request.OTSRequest request)
        {
            var requestReal = (Request.SearchRequest)request;
            var builder = PB.SearchRequest.CreateBuilder();

            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.IndexName);

            if (requestReal.SearchQuery != null)
            {
                builder.SetSearchQuery(EncodeSearchQuery(requestReal.SearchQuery).ToByteString());
            }

            if (requestReal.ColumnsToGet != null)
            {
                builder.SetColumnsToGet(EncodeColumsToGet(requestReal.ColumnsToGet));
            }

            if (requestReal.RoutingValues != null)
            {
                List<ByteString> routingValues = new List<ByteString>();
                foreach (var pk in requestReal.RoutingValues)
                {
                    try
                    {
                        routingValues.Add(ByteString.CopyFrom(PlainBufferBuilder.BuildPrimaryKeyWithHeader(pk)));
                    }
                    catch (Exception e)
                    {
                        throw new OTSClientException("build plain buffer fail,msg:" + e.Message);
                    }
                }
                builder.AddRangeRoutingValues(routingValues);
            }

            if (requestReal.TimeoutInMillisecond > 0)
            {
                builder.SetTimeoutMs(requestReal.TimeoutInMillisecond);
            }

            return builder.Build();
        }

        private PB.ColumnsToGet EncodeColumsToGet(DataModel.Search.ColumnsToGet columnsToGet)
        {
            var builder = PB.ColumnsToGet.CreateBuilder();
            if (columnsToGet.ReturnAll)
            {
                builder.SetReturnType(ColumnReturnType.RETURN_ALL);
            }
            else if (columnsToGet.ReturnAllFromIndex)
            {
                builder.SetReturnType(ColumnReturnType.RETURN_ALL_FROM_INDEX);
            }
            else if (columnsToGet.Columns.Count > 0)
            {
                builder.SetReturnType(ColumnReturnType.RETURN_SPECIFIED);
                builder.AddRangeColumnNames(columnsToGet.Columns);
            }
            else
            {
                builder.SetReturnType(ColumnReturnType.RETURN_NONE);
            }
            return builder.Build();
        }

        private PB.SearchQuery EncodeSearchQuery(DataModel.Search.SearchQuery searchQuery)
        {
            var builder = PB.SearchQuery.CreateBuilder();
            if (searchQuery.Limit.HasValue)
            {
                builder.SetLimit(searchQuery.Limit.Value);
            }

            if (searchQuery.Offset.HasValue)
            {
                builder.SetOffset(searchQuery.Offset.Value);
            }

            if (searchQuery.Collapse != null)
            {
                builder.SetCollapse(EncodeCollapse(searchQuery.Collapse));
            }

            if (searchQuery.Query != null)
            {
                builder.SetQuery(SearchQueryBuilder.BuildQuery(searchQuery.Query));
            }

            if (searchQuery.Sort != null)
            {
                builder.SetSort(SearchSortBuilder.BuildSort(searchQuery.Sort));
            }

            builder.SetGetTotalCount(searchQuery.GetTotalCount);

            if (searchQuery.Token != null)
            {
                builder.SetToken(ByteString.CopyFrom(searchQuery.Token));
            }

            if (searchQuery.AggregationList != null && searchQuery.AggregationList.Count != 0)
            {
                builder.SetAggs(SearchAggregationBuilder.BuildAggregations(searchQuery.AggregationList));
            }

            if (searchQuery.GroupByList != null && searchQuery.GroupByList.Count != 0)
            {
                builder.SetGroupBys(SearchGroupByBuilder.BuildGroupBys(searchQuery.GroupByList));
            }

            return builder.Build();
        }

        private PB.Collapse EncodeCollapse(DataModel.Search.Collapse collapse)
        {
            var builder = PB.Collapse.CreateBuilder();
            if (collapse.FieldName != null)
            {
                builder.SetFieldName(collapse.FieldName);
            }
            return builder.Build();
        }

        private PB.IndexSchema EncodeIndexSchema(DataModel.Search.IndexSchema schema)
        {
            var builder = PB.IndexSchema.CreateBuilder();

            if (schema.IndexSetting != null)
            {
                builder.SetIndexSetting(EncodeIndexSetting(schema.IndexSetting));
            }
            else
            {
                builder.SetIndexSetting(EncodeIndexSetting(new DataModel.Search.IndexSetting()));
            }
            if (schema.FieldSchemas != null)
            {
                for (var i = 0; i < schema.FieldSchemas.Count; i++)
                {
                    builder.FieldSchemasList.Add(EncodingFieldSchema(schema.FieldSchemas[i]));
                }
            }
            if (schema.IndexSort != null)
            {
                builder.SetIndexSort(SearchSortBuilder.BuildSort(schema.IndexSort));
            }
            return builder.Build();
        }

        private PB.FieldSchema EncodingFieldSchema(DataModel.Search.FieldSchema fieldSchema)
        {
            var builder = PB.FieldSchema.CreateBuilder();
            builder.SetFieldName(fieldSchema.FieldName);
            builder.SetFieldType(EncodingFieldType(fieldSchema.FieldType));

            if (fieldSchema.FieldType != DataModel.Search.FieldType.NESTED)
            {
                builder.Index = fieldSchema.index;
                builder.DocValues = fieldSchema.EnableSortAndAgg;
                builder.Store = fieldSchema.Store;
                builder.IsArray = fieldSchema.IsArray;
            }

            builder.IndexOptions = EncodingIndexOptions(fieldSchema.IndexOptions);

            if (fieldSchema.Analyzer.HasValue)
            {
                builder.Analyzer = EncodingAnalyzer(fieldSchema.Analyzer.Value);
            }

            if (fieldSchema.Analyzer.HasValue && fieldSchema.AnalyzerParameter != null)
            {
                switch (fieldSchema.Analyzer)
                {
                    case Analyzer.SingleWord:
                    case Analyzer.Split:
                    case Analyzer.Fuzzy:
                        builder.SetAnalyzerParameter(fieldSchema.AnalyzerParameter.Serialize());
                        break;
                    default:
                        break;
                }
            }

            if (fieldSchema.SubFieldSchemas != null)
            {
                for (var i = 0; i < fieldSchema.SubFieldSchemas.Count; i++)
                {
                    builder.AddFieldSchemas(EncodingFieldSchema(fieldSchema.SubFieldSchemas[i]));
                }
            }

            if (fieldSchema.IsVirtualField.HasValue)
            {
                builder.SetIsVirtualField(fieldSchema.IsVirtualField.Value);
            }

            if (fieldSchema.SourceFieldNames != null)
            {
                foreach (string sourceFieldName in fieldSchema.SourceFieldNames)
                {
                    builder.AddSourceFieldNames(sourceFieldName);
                }
            }

            if (fieldSchema.DateFormats != null)
            {
                foreach (string dataFormat in fieldSchema.DateFormats)
                {
                    builder.AddDateFormates(dataFormat);
                }
            }

            return builder.Build();
        }

        private string EncodingAnalyzer(Analyzer analyzer)
        {
            switch (analyzer)
            {
                case Analyzer.MaxWord:
                    return "max_word";
                case Analyzer.SingleWord:
                    return "single_word";
                case Analyzer.Fuzzy:
                    return "fuzzy";
                case Analyzer.MinWord:
                    return "min_word";
                case Analyzer.Split:
                    return "split";
                default:
                    throw new OTSClientException(
                        String.Format("Invalid Analyzer {0}", analyzer.ToString())
                    );
            }
        }

        private PB.IndexOptions EncodingIndexOptions(DataModel.Search.IndexOptions indexOptions)
        {
            switch (indexOptions)
            {
                case DataModel.Search.IndexOptions.DOCS:
                    return PB.IndexOptions.DOCS;
                case DataModel.Search.IndexOptions.FREQS:
                    return PB.IndexOptions.FREQS;
                case DataModel.Search.IndexOptions.OFFSETS:
                    return PB.IndexOptions.OFFSETS;
                case DataModel.Search.IndexOptions.POSITIONS:
                    return PB.IndexOptions.POSITIONS;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid IndexOptions {0}", indexOptions.ToString())
                    );
            }
        }

        private PB.FieldType EncodingFieldType(DataModel.Search.FieldType fieldType)
        {
            switch (fieldType)
            {
                case DataModel.Search.FieldType.BOOLEAN:
                    return PB.FieldType.BOOLEAN;
                case DataModel.Search.FieldType.DOUBLE:
                    return PB.FieldType.DOUBLE;
                case DataModel.Search.FieldType.GEO_POINT:
                    return PB.FieldType.GEO_POINT;
                case DataModel.Search.FieldType.KEYWORD:
                    return PB.FieldType.KEYWORD;
                case DataModel.Search.FieldType.LONG:
                    return PB.FieldType.LONG;
                case DataModel.Search.FieldType.NESTED:
                    return PB.FieldType.NESTED;
                case DataModel.Search.FieldType.TEXT:
                    return PB.FieldType.TEXT;
                case DataModel.Search.FieldType.DATE:
                    return PB.FieldType.DATE;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid FieldType {0}", fieldType.ToString())
                    );
            }
        }

        private PB.Query EncodingIQuery(DataModel.Search.Query.IQuery query)
        {
            var builder = PB.Query.CreateBuilder();

            if (query is DataModel.Search.Query.IQuery)
            {
                builder.SetQuery_(query.Serialize());
            }

            switch (query.GetQueryType())
            {
                case DataModel.Search.Query.QueryType.QueryType_MatchQuery:
                    builder.SetType(PB.QueryType.MATCH_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_MatchPhraseQuery:
                    builder.SetType(PB.QueryType.MATCH_PHRASE_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_TermQuery:
                    builder.SetType(PB.QueryType.TERM_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_RangeQuery:
                    builder.SetType(PB.QueryType.RANGE_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_PrefixQuery:
                    builder.SetType(PB.QueryType.PREFIX_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_BoolQuery:
                    builder.SetType(PB.QueryType.BOOL_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_ConstScoreQuery:
                    builder.SetType(PB.QueryType.CONST_SCORE_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_FunctionScoreQuery:
                    builder.SetType(PB.QueryType.FUNCTION_SCORE_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_NestedQuery:
                    builder.SetType(PB.QueryType.NESTED_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_WildcardQuery:
                    builder.SetType(PB.QueryType.WILDCARD_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_MatchAllQuery:
                    builder.SetType(PB.QueryType.MATCH_ALL_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_GeoBoundingBoxQuery:
                    builder.SetType(PB.QueryType.GEO_BOUNDING_BOX_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_GeoDistanceQuery:
                    builder.SetType(PB.QueryType.GEO_DISTANCE_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_GeoPolygonQuery:
                    builder.SetType(PB.QueryType.GEO_POLYGON_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_TermsQuery:
                    builder.SetType(PB.QueryType.TERMS_QUERY);
                    return builder.Build();
                case DataModel.Search.Query.QueryType.QueryType_ExistsQuery:
                    builder.SetType(PB.QueryType.EXISTS_QUERY);
                    return builder.Build();
                default:
                    throw new OTSClientException(String.Format(
                           "Invalid Query Type: {0}", query.GetQueryType().ToString()
                       ));
            }
        }

        private PB.ScanQuery EncodingScanQuery(DataModel.Search.ScanQuery scanQuery)
        {
            var builder = PB.ScanQuery.CreateBuilder();
            if (scanQuery.Query != null)
            {
                builder.Query = EncodingIQuery(scanQuery.Query);
            }
            if (scanQuery.Limit.HasValue)
            {
                builder.SetLimit(scanQuery.Limit.Value);
            }

            if (scanQuery.MaxParallel.HasValue)
            {
                builder.SetMaxParalle(scanQuery.MaxParallel.Value);
            }

            if (scanQuery.CurrentParallelId.HasValue)
            {
                builder.SetCurrentParallelId(scanQuery.CurrentParallelId.Value);
            }


            if (scanQuery.AliveTime.HasValue)
            {
                builder.SetAliveTime(scanQuery.AliveTime.Value);
            }

            if (scanQuery.Token != null)
            {
                builder.SetToken(ByteString.CopyFrom(scanQuery.Token));
            }

            return builder.Build();
        }

        private PB.IndexSetting EncodeIndexSetting(DataModel.Search.IndexSetting indexSetting)
        {
            var builder = PB.IndexSetting.CreateBuilder();
            if (indexSetting != null)
            {
                if (indexSetting.RoutingFields != null)
                {
                    for (var i = 0; i < indexSetting.RoutingFields.Count; i++)
                    {
                        builder.SetRoutingFields(i, indexSetting.RoutingFields[i]);
                    }
                }
                builder.SetNumberOfShards(DEFAULT_NUMBER_OF_SHARDS);
            }
            return builder.Build();
        }


        private IMessage EncodeDescribeSearchIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.DescribeSearchIndexRequest)request;
            var builder = PB.DescribeSearchIndexRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.IndexName);
            return builder.Build();
        }

        private IMessage EncodeDeleteSearchIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteSearchIndexRequest)request;
            var builder = PB.DeleteSearchIndexRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.IndexName);
            return builder.Build();
        }

        private IMessage EncodeParallelScan(Request.OTSRequest request)
        {
            var requestReal = (Request.ParallelScanRequest)request;
            var builder = PB.ParallelScanRequest.CreateBuilder();

            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.IndexName);

            if (requestReal.ScanQuery != null)
            {
                builder.SetScanQuery(EncodingScanQuery(requestReal.ScanQuery));
            }

            if (requestReal.ColumnToGet != null)
            {
                builder.SetColumnsToGet(EncodeColumsToGet(requestReal.ColumnToGet));
            }

            if (requestReal.SessionId != null)
            {
                builder.SetSessionId(ByteString.CopyFrom(requestReal.SessionId));
            }

            builder.SetTimeoutMs(requestReal.TimeoutInMillisecond);

            return builder.Build();
        }

        private IMessage EncodeUpdateSearchIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateSearchIndexRequest)request;
            var builder = PB.UpdateSearchIndexRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetIndexName(requestReal.Indexname);

            if (requestReal.TimeToLive.HasValue)
            {
                builder.SetTimeToLive(requestReal.TimeToLive.Value);
            }

            return builder.Build();
        }

        private IMessage EncodeCreateGlobalIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.CreateGlobalIndexRequest)request;
            var builder = PB.CreateIndexRequest.CreateBuilder();
            builder.SetMainTableName(requestReal.MainTableName);
            builder.SetIncludeBaseData(requestReal.IncludeBaseData);
            builder.SetIndexMeta(EncodeIndexMeta(requestReal.IndexMeta));

            return builder.Build();
        }

        public IMessage EncodeDeleteGlobalIndex(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteGlobalIndexRequest)request;
            var builder = PB.DropIndexRequest.CreateBuilder();
            builder.SetMainTableName(requestReal.MainTableName);
            builder.SetIndexName(requestReal.IndexName);

            return builder.Build();
        }

        public IMessage EncodeSQLQuery(Request.OTSRequest request)
        {
            var requsetReal = (Request.SQLQueryRequest)request;
            var builder = PB.SQLQueryRequest.CreateBuilder();

            if (requsetReal.Query != null)
            {
                builder.SetQuery(requsetReal.Query);
            }

            if (requsetReal.SQLPayloadVersion.HasValue)
            {
                builder.SetVersion(ToPBSQLPayloadVersion(requsetReal.SQLPayloadVersion.Value));
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

            if (tableOptions.BloomFilterType.HasValue)
            {
                builder.SetBloomFilterType(EncodeBloomFilterType(tableOptions.BloomFilterType.Value));
            }

            if (tableOptions.AllowUpdate.HasValue)
            {
                builder.SetAllowUpdate(tableOptions.AllowUpdate.Value);
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
                        String.Format("Invalid bloomFilterType {0}", bloomFilterType.ToString())
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

        private PB.IndexMeta EncodeIndexMeta(DataModel.IndexMeta indexMeta)
        {
            var builder = PB.IndexMeta.CreateBuilder();
            builder.Name = indexMeta.IndexName;
            builder.IndexType = EncodeIndexType(indexMeta.IndexType);
            builder.IndexUpdateMode = EncodeIndexUpdateMode(indexMeta.IndexUpdateModel);
            if (indexMeta.PrimaryKey != null)
            {
                for (int i = 0; i < indexMeta.PrimaryKey.Count; i++)
                {
                    builder.AddPrimaryKey(indexMeta.PrimaryKey[i]);
                }
            }

            if (indexMeta.DefinedColumns != null)
            {
                for (int i = 0; i < indexMeta.DefinedColumns.Count; i++)
                {
                    builder.AddDefinedColumn(indexMeta.DefinedColumns[i]);
                }
            }

            return builder.Build();
        }

        private PB.IndexType EncodeIndexType(DataModel.IndexType indexType)
        {
            switch (indexType)
            {
                case DataModel.IndexType.IT_GLOBAL_INDEX:
                    return PB.IndexType.IT_GLOBAL_INDEX;
                default:
                    throw new OTSClientException(String.Format(
                       "Invalid indexType {0}", indexType.ToString()
                   ));
            }
        }

        private PB.IndexUpdateMode EncodeIndexUpdateMode(DataModel.IndexUpdateMode indexUpdateModel)
        {
            switch (indexUpdateModel)
            {
                case DataModel.IndexUpdateMode.IUM_ASYNC_INDEX:
                    return PB.IndexUpdateMode.IUM_ASYNC_INDEX;
                default:
                    throw new OTSClientException(String.Format(
                       "Invalid indexUpdateModel {0}", indexUpdateModel.ToString()
                   ));
            }
        }

        private PB.TableMeta EncodeTableMeta(DataModel.TableMeta tableMeta)
        {
            var builder = PB.TableMeta.CreateBuilder();
            builder.SetTableName(tableMeta.TableName);
            builder.AddRangePrimaryKey(EncodePrimaryKeySchema(tableMeta.PrimaryKeySchema));
            if (tableMeta.DefinedColumnSchema != null)
            {
                builder.AddRangeDefinedColumn(EncodeDefinedColumnSchema(tableMeta.DefinedColumnSchema));
            }
            return builder.Build();
        }

        private IEnumerable<PB.DefinedColumnSchema> EncodeDefinedColumnSchema(DataModel.DefinedColumnSchema schema)
        {
            foreach (var item in schema)
            {
                yield return EncodeDefinedColumnSchemaItem(item);
            }
        }

        private PB.DefinedColumnSchema EncodeDefinedColumnSchemaItem(Tuple<string, DataModel.DefinedColumnType> schema)
        {
            var builder = PB.DefinedColumnSchema.CreateBuilder();
            builder.SetName(schema.Item1);
            builder.SetType(EncodeDeinedColumnType(schema.Item2));

            return builder.Build();
        }

        private PB.DefinedColumnType EncodeDeinedColumnType(DataModel.DefinedColumnType type)
        {
            switch (type)
            {
                case DataModel.DefinedColumnType.BINARY:
                    return DefinedColumnType.DCT_BLOB;
                case DataModel.DefinedColumnType.INTEGER:
                    return DefinedColumnType.DCT_INTEGER;
                case DataModel.DefinedColumnType.STRING:
                    return DefinedColumnType.DCT_STRING;
                case DataModel.DefinedColumnType.DOUBLE:
                    return DefinedColumnType.DCT_DOUBLE;
                case DataModel.DefinedColumnType.BOOLEAN:
                    return DefinedColumnType.DCT_BOOLEAN;

                default:
                    throw new OTSClientException(String.Format(
                           "Invalid definedColumn value type: {0}", type.ToString()
                       ));
            }
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
                        "Invalid column value type: {0}", type.ToString()
                    ));
            }
        }

        private PB.PrimaryKeyOption EncodePrimaryKeyOption(DataModel.PrimaryKeyOption option)
        {
            switch (option)
            {
                case DataModel.PrimaryKeyOption.AUTO_INCREMENT:
                    return PB.PrimaryKeyOption.AUTO_INCREMENT;
                default:
                    throw new OTSClientException(String.Format(
                        "Invalid primary key option: {0}", option.ToString()
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

        private PB.SearchIndexSplitsOptions EncodeSearchIndexOptions(DataModel.ISplitsOptions options)
        {
            var builder = PB.SearchIndexSplitsOptions.CreateBuilder();

            var searchIndexOptions = (DataModel.Search.SearchIndexSplitsOptions)options;

            if (searchIndexOptions.IndexName != null)
            {
                builder.SetIndexName(searchIndexOptions.IndexName);
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
                    throw new OTSClientException(String.Format("Invalid RowExistenceExpectation: {0}", condition.RowExistenceExpect.ToString()));
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

        private static PB.ReturnContent EncodeReturnContent(DataModel.ReturnType returnType, List<string> returnColumnNames)
        {
            PB.ReturnContent.Builder builder = PB.ReturnContent.CreateBuilder();
            builder.SetReturnType(ToPBReturnType(returnType));

            if (returnColumnNames != null)
            {
                foreach (var item in returnColumnNames)
                {
                    builder.AddReturnColumnNames(item);
                }
            }

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
            rowBuilder.SetReturnContent(EncodeReturnContent(rowChange.ReturnType, rowChange.ReturnColumnNames));
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
                case DataModel.ReturnType.RT_AFTER_MODIFY:
                    return PB.ReturnType.RT_AFTER_MODIFY;
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

        private static PB.FilterType ToPBFilterType(DataModel.Filter.FilterType filterType)
        {
            switch (filterType)
            {
                case DataModel.Filter.FilterType.SINGLE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_SINGLE_COLUMN_VALUE;
                case DataModel.Filter.FilterType.COMPOSITE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_COMPOSITE_COLUMN_VALUE;
                case DataModel.Filter.FilterType.COLUMN_PAGINATION_FILTER:
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

        private PB.SQLPayloadVersion ToPBSQLPayloadVersion(DataModel.SQL.SQLPayloadVersion version)
        {
            switch (version)
            {
                case DataModel.SQL.SQLPayloadVersion.SQLFlatBuffers:
                    return PB.SQLPayloadVersion.SQL_FLAT_BUFFERS;
                default:
                    throw new ArgumentException(string.Format("Unkown SQL payload version: {0}", version));
            }
        }
    }
}
