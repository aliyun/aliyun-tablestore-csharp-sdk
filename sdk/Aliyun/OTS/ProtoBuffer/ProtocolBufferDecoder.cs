using System;
using System.Collections.Generic;
using Google.ProtocolBuffers;
using System.IO;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.DataModel.Search;
using com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.Handler;

namespace Aliyun.OTS.ProtoBuffer
{
    public class ProtocolBufferDecoder : PipelineHandler
    {
        private delegate Response.OTSResponse ResponseDecoder(byte[] body, out IMessage message);
        private readonly Dictionary<string, ResponseDecoder> DecoderMap;

        public ProtocolBufferDecoder(PipelineHandler innerHandler) : base(innerHandler)
        {
            DecoderMap = new Dictionary<string, ResponseDecoder>() {
                { "/CreateTable",              DecodeCreateTable },
                { "/DeleteTable",              DecodeDeleteTable },
                { "/UpdateTable",              DecodeUpdateTable },
                { "/DescribeTable",            DecodeDescribeTable },
                { "/ListTable",                DecodeListTable },

                { "/PutRow",                   DecodePutRow },
                { "/GetRow",                   DecodeGetRow },
                { "/UpdateRow",                DecodeUpdateRow },
                { "/DeleteRow",                DecodeDeleteRow },

                { "/BatchWriteRow",            DecodeBatchWriteRow },
                { "/BatchGetRow",              DecodeBatchGetRow },
                { "/GetRange",                 DecodeGetRange },

                { "/ListSearchIndex",          DecodeListSearchIndex },
                { "/CreateSearchIndex",        DecodeCreateSearchIndex },
                { "/ComputeSplits",            DecodeComputeSplits},
                { "/DescribeSearchIndex",      DecodeDescribeSearchIndex },
                { "/DeleteSearchIndex",        DecodeDeleteSearchIndex },
                { "/ParallelScan",             DecodeParallelScan },
                { "/UpdateSearchIndex" ,       DecodeUpdateSearchIndex },
                { "/Search",                   DecodeSearch },

                { "/CreateIndex",              DecodeCreateGlobalIndex },
                { "/DropIndex",                DecodeDeleteGlobalIndex },

                { "/SQLQuery",                 DecodeSQLQuery },
            };
        }

        public override void HandleBefore(Context context)
        {
            InnerHandler.HandleBefore(context);
        }

        public override void HandleAfter(Context context)
        {
            InnerHandler.HandleAfter(context);
            IMessage message;

            context.OTSReponse = DecoderMap[context.APIName](context.HttpResponseBody, out message);

            SetRequestIDForResponse(context);

            LogEncodedMessage(context, message);
        }

        private void SetRequestIDForResponse(Context context)
        {
            if (context.HttpResponseHeaders.ContainsKey("x-ots-requestid"))
            {
                context.OTSReponse.RequestID = context.HttpResponseHeaders["x-ots-requestid"];
            }
        }

        private void LogEncodedMessage(Context context, IMessage message)
        {
            if (context.ClientConfig.OTSDebugLogHandler != null)
            {
                string requestID = "";
                if (context.HttpResponseHeaders.ContainsKey("x-ots-requestid"))
                {
                    requestID = context.HttpResponseHeaders["x-ots-requestid"];
                }
                var msgString = String.Format("OTS Response API: {0} RequestID: {1} Protobuf: {2}\n",
                                              context.APIName,
                                              requestID,
                                              TextFormat.PrintToString(message));

                context.ClientConfig.OTSDebugLogHandler(msgString);
            }
        }

        private Response.OTSResponse DecodeCreateTable(byte[] body, out IMessage _message)
        {
            var response = new Response.CreateTableResponse();
            var builder = PB.CreateTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeDeleteTable(byte[] body, out IMessage _message)
        {
            var response = new Response.DeleteTableResponse();
            var builder = PB.DeleteTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeUpdateTable(byte[] body, out IMessage _message)
        {
            var builder = PB.UpdateTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            var response = new Response.UpdateTableResponse(
                ParseReservedThroughputDetails(message.ReservedThroughputDetails)
            );
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeListTable(byte[] body, out IMessage _message)
        {
            var response = new Response.ListTableResponse
            {
                TableNames = new List<string>()
            };

            var builder = PB.ListTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            for (int i = 0; i < message.TableNamesCount; i++)
            {
                response.TableNames.Add(message.GetTableNames(i));
            }
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeDescribeTable(byte[] body, out IMessage _message)
        {
            var response = new Response.DescribeTableResponse();
            var builder = PB.DescribeTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            response.TableMeta = ParseTableMeta(message.TableMeta);
            response.ReservedThroughputDetails = ParseReservedThroughputDetails(message.ReservedThroughputDetails);
            response.StreamDetails = ParseStreamDetails(message.StreamDetails);
            response.TableOptions = ParseTableOptions(message.TableOptions);
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodePutRow(byte[] body, out IMessage _message)
        {
            var builder = PB.PutRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            DataModel.Row row = null;
            if (message.HasRow && !message.Row.IsEmpty)
            {
                row = ParseRow(message.Row);
            }
            else
            {
                row = new DataModel.Row(new DataModel.PrimaryKey(), new List<DataModel.Column>());
            }

            var response = new Response.PutRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit),
                row
            );
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeGetRow(byte[] body, out IMessage _message)
        {
            var builder = PB.GetRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            DataModel.Row row = null;

            if (message.HasRow && !message.Row.IsEmpty)
            {
                row = ParseRow(message.Row);
            }
            else
            {
                row = new DataModel.Row(new DataModel.PrimaryKey(), new List<DataModel.Column>());
            }

            var primaryKey = row.GetPrimaryKey();
            var columns = row.GetColumns();

            var response = new Response.GetRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit),
                row
            );

            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeUpdateRow(byte[] body, out IMessage _message)
        {
            var builder = PB.UpdateRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            DataModel.Row row = null;

            if (message.HasRow && !message.Row.IsEmpty)
            {
                row = ParseRow(message.Row);
            }
            else
            {
                row = new DataModel.Row(new DataModel.PrimaryKey(), new List<DataModel.Column>());
            }

            var response = new Response.UpdateRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit),
                row
            );
            _message = message;
            return response;
        }



        private Response.OTSResponse DecodeDeleteRow(byte[] body, out IMessage _message)
        {
            var builder = PB.DeleteRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            DataModel.Row row = null;
            if (message.HasRow && !message.Row.IsEmpty)
            {
                row = ParseRow(message.Row);
            }
            else
            {
                row = new DataModel.Row(new DataModel.PrimaryKey(), new List<DataModel.Column>());
            }

            var response = new Response.DeleteRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit),
                row
            );
            _message = message;
            return response;
        }

        private DataModel.Row ParseRow(ByteString row)
        {
            PB.PlainBufferCodedInputStream inputStream = new PB.PlainBufferCodedInputStream(row.CreateCodedInput());
            List<PB.PlainBufferRow> rows = inputStream.ReadRowsWithHeader();
            if (rows.Count != 1)
            {
                throw new IOException("Expect only returns one row. Row count: " + rows.Count);
            }

            return PB.PlainBufferConversion.ToRow(rows[0]) as DataModel.Row;
        }

        private Response.OTSResponse DecodeBatchWriteRow(byte[] body, out IMessage _message)
        {
            var builder = PB.BatchWriteRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            var response = new Response.BatchWriteRowResponse();

            foreach (var table in message.TablesList)
            {
                var item = ParseTableInBatchWriteRowResponse(table);
                response.TableRespones.Add(table.TableName, item);
            }

            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeBatchGetRow(byte[] body, out IMessage _message)
        {
            var builder = PB.BatchGetRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            var response = new Response.BatchGetRowResponse();

            foreach (var table in message.TablesList)
            {
                response.Add(table.TableName, ParseTableInBatchGetRowResponse(table));
            }
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeGetRange(byte[] body, out IMessage _message)
        {
            var builder = PB.GetRangeResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            var response = new Response.GetRangeResponse
            {
                ConsumedCapacityUnit = ParseCapacityUnit(message.Consumed.CapacityUnit)
            };

            if (!message.HasNextStartPrimaryKey)
            {
                response.NextPrimaryKey = null;
            }
            else
            {
                var inputStream = new PB.PlainBufferCodedInputStream(message.NextStartPrimaryKey.CreateCodedInput());
                var rows = inputStream.ReadRowsWithHeader();
                if (rows.Count != 1)
                {
                    throw new IOException("Expect only one row return. Row count: " + rows.Count);
                }

                PB.PlainBufferRow row = rows[0];
                if (row.HasDeleteMarker() || row.HasCells())
                {
                    throw new IOException("The next primary key should only have primary key: " + row);
                }

                response.NextPrimaryKey = PB.PlainBufferConversion.ToPrimaryKey(row.GetPrimaryKey());
            }


            if (message.HasRows && !message.Rows.IsEmpty)
            {
                List<DataModel.Row> rows = new List<DataModel.Row>();
                var inputStream = new PB.PlainBufferCodedInputStream(message.Rows.CreateCodedInput());

                List<PB.PlainBufferRow> pbRows = inputStream.ReadRowsWithHeader();
                foreach (var pbRow in pbRows)
                {

                    rows.Add((DataModel.Row)PB.PlainBufferConversion.ToRow(pbRow));
                }

                response.RowDataList = rows;
            }

            if (message.HasNextToken)
            {
                response.NextToken = message.NextToken.ToByteArray();
            }

            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeListSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.ListSearchIndexResponse
            {
                IndexInfos = new List<SearchIndexInfo>()
            };

            var builder = PB.ListSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            for (int i = 0; i < message.IndicesCount; i++)
            {
                PB.IndexInfo indexInfo = message.GetIndices(i);
                SearchIndexInfo searchIndexInfo = new SearchIndexInfo();
                searchIndexInfo.TableName = indexInfo.TableName;
                searchIndexInfo.IndexName = indexInfo.IndexName;
                response.IndexInfos.Add(searchIndexInfo);
            }
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeCreateSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.CreateSearchIndexResponse();
            var builder = PB.CreateSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeComputeSplits(byte[] body, out IMessage _message)
        {
            var response = new Response.ComputeSplitsResponse();
            var builder = PB.ComputeSplitsResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;

            if (message.HasSessionId)
            {
                response.SessionId = message.SessionId.ToByteArray();
            }

            if (message.HasSplitsSize)
            {
                response.SplitsSize = message.SplitsSize;
            }

            return response;
        }

        private Response.OTSResponse DecodeDeleteSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.DeleteSearchIndexResponse();
            var builder = PB.DeleteSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeParallelScan(byte[] body, out IMessage _message)
        {
            var response = new Response.ParallelScanResponse();
            var builder = PB.ParallelScanResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;

            if (message.RowsList != null && message.RowsCount != 0)
            {
                response.Rows = new List<DataModel.Row>();

                foreach (ByteString row in message.RowsList)
                {
                    response.Rows.Add(ParseRow(row));
                }
            }

            if (message.HasNextToken)
            {
                response.NextToken = message.NextToken.ToByteArray();
            }

            response.BodyBytes = message.SerializedSize;

            return response;
        }

        private Response.OTSResponse DecodeUpdateSearchIndex(byte[] body, out IMessage _message)
        {
            var builder = PB.UpdateSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return new Response.UpdateSearchIndexResponse();
        }

        private Response.OTSResponse DecodeSearch(byte[] body, out IMessage _message)
        {
            var response = new Response.SearchResponse();
            var builder = PB.SearchResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;

            response.TotalCount = message.TotalHits;
            response.IsAllSuccess = message.IsAllSucceeded;
            response.BodyBytes = message.SerializedSize;

            response.Rows = new List<DataModel.Row>();
            foreach (var item in message.RowsList)
            {
                PlainBufferCodedInputStream coded = new PlainBufferCodedInputStream(item.CreateCodedInput());
                List<PlainBufferRow> plainBufferRows = coded.ReadRowsWithHeader();
                if (plainBufferRows.Count != 1)
                {
                    throw new IOException("Expect only returns one row. Row count: " + plainBufferRows.Count);
                }
                var row = PlainBufferConversion.ToRow(plainBufferRows[0]);
                response.Rows.Add(row as DataModel.Row);
            }
            if (message.HasNextToken)
            {
                response.NextToken = message.NextToken.ToByteArray();
            }

            if (message.HasAggs)
            {
                response.AggregationResults = SearchAggregationResultBuilder.BuildAggregationResultsFromByteString(message.Aggs);
            }

            if (message.HasGroupBys)
            {
                response.GroupByResults = SearchGroupByResultBuilder.BuildGroupByResultsFromByteString(message.GroupBys);
            }

            return response;
        }

        private Response.OTSResponse DecodeDescribeSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.DescribeSearchIndexResponse();
            var builder = PB.DescribeSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;

            if (message.HasSchema)
            {
                response.Schema = ParseIndexSchema(message.Schema);
            }

            if (message.HasSyncStat)
            {
                response.SyncStat = ParseSyncStat(message.SyncStat);
            }

            if (message.HasMeteringInfo)
            {
                response.MeteringInfo = ParseMeteringInfo(message.MeteringInfo);
            }

            if (message.HasBrotherIndexName)
            {
                response.BrotherIndexName = message.BrotherIndexName;
            }

            if (message.HasCreateTime)
            {
                response.CreateTime = message.CreateTime;
            }

            if (message.HasTimeToLive)
            {
                response.TimeToLive = message.TimeToLive;
            }
            return response;
        }

        private DataModel.Search.MeteringInfo ParseMeteringInfo(PB.MeteringInfo meteringInfo)
        {
            var searchMeteringInfo = new DataModel.Search.MeteringInfo();

            if (meteringInfo.HasReservedReadCu)
            {
                searchMeteringInfo.ReservedThroughput = new DataModel.ReservedThroughput(new DataModel.CapacityUnit((int)meteringInfo.ReservedReadCu));
            }

            if (meteringInfo.HasRowCount)
            {
                searchMeteringInfo.RowCount = meteringInfo.RowCount;
            }

            if (meteringInfo.HasStorageSize)
            {
                searchMeteringInfo.StorageSize = meteringInfo.StorageSize;
            }

            if (meteringInfo.HasTimestamp)
            {
                searchMeteringInfo.Timestamp = meteringInfo.Timestamp;
            }

            return searchMeteringInfo;
        }

        private DataModel.Search.SyncStat ParseSyncStat(PB.SyncStat syncStat)
        {
            var ret = new DataModel.Search.SyncStat();
            ret.CurrentSyncTimestamp = syncStat.CurrentSyncTimestamp;
            ret.SyncPhase = ParseSyncPhase(syncStat.SyncPhase);
            return ret;
        }

        private DataModel.Search.SyncPhase ParseSyncPhase(PB.SyncPhase syncPhase)
        {
            switch (syncPhase)
            {
                case PB.SyncPhase.FULL:
                    return DataModel.Search.SyncPhase.FULL;
                case PB.SyncPhase.INCR:
                    return DataModel.Search.SyncPhase.INCR;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid indexOptions SyncPhase type {0}", syncPhase)
                    );
            }
        }

        private DataModel.Search.IndexSchema ParseIndexSchema(PB.IndexSchema indexSchema)
        {
            var ret = new DataModel.Search.IndexSchema();
            ret.IndexSetting = ParseIndexSetting(indexSchema.IndexSetting);
            ret.FieldSchemas = new List<DataModel.Search.FieldSchema>();
            foreach (var item in indexSchema.FieldSchemasList)
            {
                ret.FieldSchemas.Add(ParseFieldSchema(item));
            }
            return ret;
        }

        private DataModel.Search.FieldSchema ParseFieldSchema(PB.FieldSchema fieldSchema)
        {
            var ret = new DataModel.Search.FieldSchema(fieldSchema.FieldName, ParseFieldType(fieldSchema.FieldType));

            if (fieldSchema.HasAnalyzer)
            {
                ret.Analyzer = ParseAnalyzer(fieldSchema.Analyzer);
            }

            ret.EnableSortAndAgg = fieldSchema.DocValues;
            ret.index = fieldSchema.Index;
            ret.Store = fieldSchema.Store;
            ret.IsArray = fieldSchema.IsArray;

            if (fieldSchema.FieldType != PB.FieldType.NESTED)
            {
                ret.IndexOptions = ParseIndexOption(fieldSchema.IndexOptions);
            }

            if (fieldSchema.HasAnalyzerParameter && fieldSchema.HasAnalyzer)
            {
                try
                {
                    Analyzer analyzer = ParseAnalyzer(fieldSchema.Analyzer);
                    switch (analyzer)
                    {
                        case Analyzer.SingleWord:
                            ret.AnalyzerParameter = ParseAnalyzerParameter(PB.SingleWordAnalyzerParameter.ParseFrom(fieldSchema.AnalyzerParameter));
                            break;
                        case Analyzer.Fuzzy:
                            ret.AnalyzerParameter = ParseAnalyzerParameter(PB.FuzzyAnalyzerParameter.ParseFrom(fieldSchema.AnalyzerParameter));
                            break;
                        case Analyzer.Split:
                            ret.AnalyzerParameter = ParseAnalyzerParameter(PB.SplitAnalyzerParameter.ParseFrom(fieldSchema.AnalyzerParameter));
                            break;
                    }
                }
                catch (InvalidProtocolBufferException e)
                {
                    throw new OTSClientException("failed to parse analyzer parameter: " + e.Message);
                }
            }

            if (fieldSchema.FieldSchemasList != null && fieldSchema.FieldSchemasCount != 0)
            {
                ret.SubFieldSchemas = new List<DataModel.Search.FieldSchema>();
                foreach (var item in fieldSchema.FieldSchemasList)
                {
                    ret.SubFieldSchemas.Add(ParseFieldSchema(item));
                }
            }

            if (fieldSchema.HasIsVirtualField)
            {
                ret.IsVirtualField = fieldSchema.IsVirtualField;
            }

            if (fieldSchema.SourceFieldNamesList != null && fieldSchema.SourceFieldNamesCount != 0)
            {
                ret.SourceFieldNames = new List<string>(fieldSchema.SourceFieldNamesList);
            }

            if (fieldSchema.DateFormatesList != null && fieldSchema.DateFormatesCount != 0)
            {
                ret.DateFormats = new List<string>(fieldSchema.DateFormatesList);
            }

            return ret;
        }

        private DataModel.Search.Analysis.SingleWordAnalyzerParameter ParseAnalyzerParameter(PB.SingleWordAnalyzerParameter analyzerParameter)
        {
            DataModel.Search.Analysis.SingleWordAnalyzerParameter ret = new DataModel.Search.Analysis.SingleWordAnalyzerParameter();

            if (analyzerParameter.HasCaseSensitive)
            {
                ret.CaseSensitive = analyzerParameter.CaseSensitive;
            }

            if (analyzerParameter.HasDelimitWord)
            {
                ret.DelimitWord = analyzerParameter.DelimitWord;
            }

            return ret;
        }

        private DataModel.Search.Analysis.FuzzyAnalyzerParameter ParseAnalyzerParameter(PB.FuzzyAnalyzerParameter analyzerParameter)
        {
            DataModel.Search.Analysis.FuzzyAnalyzerParameter ret = new DataModel.Search.Analysis.FuzzyAnalyzerParameter();

            if (analyzerParameter.HasMaxChars)
            {
                ret.MaxChars = analyzerParameter.MaxChars;
            }

            if (analyzerParameter.HasMinChars)
            {
                ret.MinChars = analyzerParameter.MinChars;
            }

            return ret;
        }

        private DataModel.Search.Analysis.SplitAnalyzerParameter ParseAnalyzerParameter(PB.SplitAnalyzerParameter analyzerParameter)
        {
            DataModel.Search.Analysis.SplitAnalyzerParameter ret = new DataModel.Search.Analysis.SplitAnalyzerParameter();

            if (analyzerParameter.HasDelimiter)
            {
                ret.Delimiter = analyzerParameter.Delimiter;
            }

            return ret;
        }

        private DataModel.Search.IndexOptions ParseIndexOption(PB.IndexOptions indexOptions)
        {
            switch (indexOptions)
            {
                case PB.IndexOptions.DOCS:
                    return DataModel.Search.IndexOptions.DOCS;
                case PB.IndexOptions.FREQS:
                    return DataModel.Search.IndexOptions.FREQS;
                case PB.IndexOptions.OFFSETS:
                    return DataModel.Search.IndexOptions.OFFSETS;
                case PB.IndexOptions.POSITIONS:
                    return DataModel.Search.IndexOptions.POSITIONS;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid indexOptions type {0}", indexOptions)
                    );
            }
        }

        private DataModel.Search.FieldType ParseFieldType(PB.FieldType fieldType)
        {
            switch (fieldType)
            {
                case PB.FieldType.BOOLEAN:
                    return DataModel.Search.FieldType.BOOLEAN;
                case PB.FieldType.DOUBLE:
                    return DataModel.Search.FieldType.DOUBLE;
                case PB.FieldType.GEO_POINT:
                    return DataModel.Search.FieldType.GEO_POINT;
                case PB.FieldType.KEYWORD:
                    return DataModel.Search.FieldType.KEYWORD;
                case PB.FieldType.LONG:
                    return DataModel.Search.FieldType.LONG;
                case PB.FieldType.NESTED:
                    return DataModel.Search.FieldType.NESTED;
                case PB.FieldType.TEXT:
                    return DataModel.Search.FieldType.TEXT;
                case PB.FieldType.DATE:
                    return DataModel.Search.FieldType.DATE;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid FieldType type {0}", fieldType)
                    );
            }
        }

        private DataModel.Search.Analyzer ParseAnalyzer(string analyzer)
        {
            switch (analyzer)
            {
                case "max_word":
                    return Analyzer.MaxWord;
                case "single_word":
                    return Analyzer.SingleWord;
                case "min_word":
                    return Analyzer.MinWord;
                case "split":
                    return Analyzer.Split;
                case "fuzzy":
                    return Analyzer.Fuzzy;
                default:
                    throw new OTSClientException(string.Format("Invalid Analyzer type {0}", analyzer));
            }
        }

        private DataModel.Search.IndexSetting ParseIndexSetting(PB.IndexSetting indexSetting)
        {
            var ret = new DataModel.Search.IndexSetting();
            foreach (var item in indexSetting.RoutingFieldsList)
            {
                ret.RoutingFields.Add(item);
            }

            return ret;
        }


        private IList<Response.BatchGetRowResponseItem> ParseTableInBatchGetRowResponse(PB.TableInBatchGetRowResponse table)
        {
            var ret = new List<Response.BatchGetRowResponseItem>();
            int index = 0;

            foreach (var row in table.RowsList)
            {
                DataModel.IRow result = null;
                if (!row.IsOk)
                {
                    ret.Add(new Response.BatchGetRowResponseItem(row.Error.Code, row.Error.Message));
                    continue;
                }

                if (row.HasRow && !row.Row.IsEmpty)
                {
                    var inputStream = new PB.PlainBufferCodedInputStream(row.Row.CreateCodedInput());
                    List<PB.PlainBufferRow> rows = inputStream.ReadRowsWithHeader();
                    if (rows.Count != 1)
                    {
                        throw new IOException("Expect only returns one row. Row count: " + rows.Count);
                    }
                    result = PB.PlainBufferConversion.ToRow(rows[0]);
                }

                Response.BatchGetRowResponseItem item = null;

                var capacityUnit = ParseCapacityUnit(row.Consumed.CapacityUnit);

                if (row.HasNextToken)
                {
                    item = new Response.BatchGetRowResponseItem(table.TableName, result, capacityUnit, index, row.NextToken.ToByteArray());
                }
                else
                {
                    item = new Response.BatchGetRowResponseItem(table.TableName, result, capacityUnit, index);
                }

                index++;

                ret.Add(item);

            }

            return ret;
        }

        private DataModel.TableMeta ParseTableMeta(PB.TableMeta tableMeta)
        {
            var schema = new DataModel.PrimaryKeySchema();

            for (int i = 0; i < tableMeta.PrimaryKeyCount; i++)
            {
                var item = tableMeta.GetPrimaryKey(i);

                schema.Add(item.Name, ParseColumnValueType(item.Type));
            }

            var definedColumnSchema = new DataModel.DefinedColumnSchema();

            for (int i = 0; i < tableMeta.DefinedColumnCount; i++)
            {
                var item = tableMeta.GetDefinedColumn(i);

                definedColumnSchema.Add(item.Name, ParseDefinedColumnValueType(item.Type));
            }

            var ret = new DataModel.TableMeta(tableMeta.TableName, schema)
            {
                DefinedColumnSchema = definedColumnSchema
            };

            return ret;
        }

        private DataModel.ColumnValueType ParseColumnValueType(PB.PrimaryKeyType type)
        {
            switch (type)
            {
                case PB.PrimaryKeyType.BINARY:
                    return DataModel.ColumnValueType.Binary;
                case PB.PrimaryKeyType.INTEGER:
                    return DataModel.ColumnValueType.Integer;
                case PB.PrimaryKeyType.STRING:
                    return DataModel.ColumnValueType.String;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid column primary key type {0}", type)
                    );
            }
        }

        private DataModel.DefinedColumnType ParseDefinedColumnValueType(PB.DefinedColumnType type)
        {
            switch (type)
            {
                case PB.DefinedColumnType.DCT_BLOB:
                    return DataModel.DefinedColumnType.BINARY; ;
                case PB.DefinedColumnType.DCT_BOOLEAN:
                    return DataModel.DefinedColumnType.BOOLEAN;
                case PB.DefinedColumnType.DCT_DOUBLE:
                    return DataModel.DefinedColumnType.DOUBLE;
                case PB.DefinedColumnType.DCT_INTEGER:
                    return DataModel.DefinedColumnType.INTEGER;
                case PB.DefinedColumnType.DCT_STRING:
                    return DataModel.DefinedColumnType.STRING;
                default:
                    throw new OTSClientException(string.Format("Invalid defined column type {0}", type));
            }
        }

        private DataModel.ReservedThroughputDetails ParseReservedThroughputDetails(PB.ReservedThroughputDetails details)
        {
            var ret = new DataModel.ReservedThroughputDetails(
                ParseCapacityUnit(details.CapacityUnit),
                details.LastIncreaseTime,
                details.LastDecreaseTime
            );

            return ret;
        }

        private DataModel.CapacityUnit ParseCapacityUnit(PB.CapacityUnit capacityUnit)
        {
            return new DataModel.CapacityUnit(capacityUnit.Read, capacityUnit.Write);
        }

        private DataModel.StreamDetails ParseStreamDetails(PB.StreamDetails streamDetails)
        {
            return new DataModel.StreamDetails(streamDetails.EnableStream)
            {
                StreamId = streamDetails.StreamId,
                LastEnableTime = streamDetails.LastEnableTime,
                ExpirationTime = streamDetails.ExpirationTime
            };
        }

        private DataModel.TableOptions ParseTableOptions(PB.TableOptions tableOptions)
        {
            DataModel.TableOptions options = new DataModel.TableOptions()
            {
                TimeToLive = tableOptions.TimeToLive,
                MaxVersions = tableOptions.MaxVersions,
                DeviationCellVersionInSec = tableOptions.DeviationCellVersionInSec,
                BlockSize = tableOptions.BlockSize,
                BloomFilterType = ParseBloomFilterType(tableOptions.BloomFilterType)
            };

            if (tableOptions.HasAllowUpdate)
            {
                options.AllowUpdate = tableOptions.AllowUpdate;
            }

            return options;
        }

        private DataModel.BloomFilterType ParseBloomFilterType(PB.BloomFilterType bloomFilterType)
        {
            switch (bloomFilterType)
            {
                case PB.BloomFilterType.CELL:
                    return DataModel.BloomFilterType.CELL;
                case PB.BloomFilterType.ROW:
                    return DataModel.BloomFilterType.ROW;
                case PB.BloomFilterType.NONE:
                    return DataModel.BloomFilterType.NONE;
                default:
                    throw new OTSClientException(
                        String.Format("Invalid bloomFilterType {0}", bloomFilterType)
                    );
            }
        }

        private Response.BatchWriteRowResponseForOneTable ParseTableInBatchWriteRowResponse(PB.TableInBatchWriteRowResponse table)
        {

            var ret = new Response.BatchWriteRowResponseForOneTable
            {
                Responses = ParseBatchWriteRowResponseItems(table.TableName, table.RowsList)
            };

            return ret;
        }

        private IList<Response.BatchWriteRowResponseItem> ParseBatchWriteRowResponseItems(string tableName, IList<PB.RowInBatchWriteRowResponse> responseItems)
        {
            var ret = new List<Response.BatchWriteRowResponseItem>();
            int index = 0;
            foreach (var responseItem in responseItems)
            {
                DataModel.IRow row = null;
                if (responseItem.IsOk)
                {
                    if (responseItem.HasRow && !responseItem.Row.IsEmpty)
                    {
                        try
                        {
                            var inputStream = new PB.PlainBufferCodedInputStream(responseItem.Row.CreateCodedInput());
                            List<PB.PlainBufferRow> rows = inputStream.ReadRowsWithHeader();
                            if (rows.Count != 1)
                            {
                                throw new IOException("Expect only returns one row. Row count: " + rows.Count);
                            }

                            row = PB.PlainBufferConversion.ToRow(rows[0]);
                        }
                        catch (Exception e)
                        {
                            throw new OTSException("Failed to parse row data." + e.Message);
                        }
                    }

                    ret.Add(new Response.BatchWriteRowResponseItem(
                        ParseCapacityUnit(responseItem.Consumed.CapacityUnit), tableName, index++, row));
                }
                else
                {
                    ret.Add(new Response.BatchWriteRowResponseItem(
                        responseItem.Error.Code, responseItem.Error.Message, tableName, index++));
                }
            }

            return ret;
        }

        private DataModel.SQL.SQLPayloadVersion ParseSQLPayloadVersion(SQLPayloadVersion version)
        {
            switch (version)
            {
                case SQLPayloadVersion.SQL_FLAT_BUFFERS:
                    return DataModel.SQL.SQLPayloadVersion.SQLFlatBuffers;
                default:
                    throw new OTSClientException(string.Format("not support SQL payload version: {0}", version.ToString()));
            }
        }

        private DataModel.SQL.SQLStatementType ParseSQLStatementType(SQLStatementType type)
        {
            switch (type)
            {
                case SQLStatementType.SQL_SELECT:
                    return DataModel.SQL.SQLStatementType.SQLSelect;
                case SQLStatementType.SQL_CREATE_TABLE:
                    return DataModel.SQL.SQLStatementType.SQLCreateTable;
                case SQLStatementType.SQL_SHOW_TABLE:
                    return DataModel.SQL.SQLStatementType.SQLShowTable;
                case SQLStatementType.SQL_DESCRIBE_TABLE:
                    return DataModel.SQL.SQLStatementType.SQLDescribeTable;
                case SQLStatementType.SQL_DROP_TABLE:
                    return DataModel.SQL.SQLStatementType.SQLDropTable;
                case SQLStatementType.SQL_ALTER_TABLE:
                    return DataModel.SQL.SQLStatementType.SQLAlterTable;
                default:
                    throw new OTSClientException(string.Format("not support SQL statement type: {0}", type.ToString()));
            }
        }

        private Response.CreateGlobalIndexResponse DecodeCreateGlobalIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.CreateGlobalIndexResponse();
            var builder = PB.CreateIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.DeleteGlobalIndexResponse DecodeDeleteGlobalIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.DeleteGlobalIndexResponse();
            var builder = PB.DropIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
        }

        private Response.SQLQueryResponse DecodeSQLQuery(byte[] body, out IMessage _message)
        {
            var response = new Response.SQLQueryResponse();
            var builder = PB.SQLQueryResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;

            if (message.HasVersion)
            {
                response.SQLPayloadVersion = ParseSQLPayloadVersion(message.Version);
            }

            response.SQLStatementType = ParseSQLStatementType(message.Type);

            if (message.HasRows)
            {
                response.Rows = message.Rows;
            }

            if (message.ConsumesCount == 0)
            {
                return response;
            }

            Dictionary<string, DataModel.ConsumedCapacity> consumendCapacityByTable = new Dictionary<string, OTS.DataModel.ConsumedCapacity>();
            foreach (TableConsumedCapacity tableConsumedCapacity in message.ConsumesList)
            {
                if (tableConsumedCapacity.HasConsumed && tableConsumedCapacity.Consumed.HasCapacityUnit)
                {
                    DataModel.ConsumedCapacity consumedCapacity = new DataModel.ConsumedCapacity(ParseCapacityUnit(tableConsumedCapacity.Consumed.CapacityUnit));
                    consumendCapacityByTable.Add(tableConsumedCapacity.TableName, consumedCapacity);
                }
            }
            response.ConsumedCapacityByTable = consumendCapacityByTable;

            return response;
        }
    }
}
