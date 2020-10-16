using System;
using System.Collections.Generic;
using Google.ProtocolBuffers;
using System.IO;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.DataModel.Search;
using com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.Handler
{
    public class ProtocolBufferDecoder : PipelineHandler
    {
        private delegate Response.OTSResponse ResponseDecoder(byte[] body, out IMessage message);
        private readonly Dictionary<string, ResponseDecoder> DecoderMap;

        public ProtocolBufferDecoder(PipelineHandler innerHandler) : base(innerHandler)
        {
            DecoderMap = new Dictionary<string, ResponseDecoder>() {
                { "/CreateTable",          DecodeCreateTable },
                { "/DeleteTable",          DecodeDeleteTable },
                { "/UpdateTable",          DecodeUpdateTable },
                { "/DescribeTable",        DecodeDescribeTable },
                { "/ListTable",            DecodeListTable },

                { "/PutRow",               DecodePutRow },
                { "/GetRow",               DecodeGetRow },
                { "/UpdateRow",            DecodeUpdateRow },
                { "/DeleteRow",            DecodeDeleteRow },

                { "/BatchWriteRow",        DecodeBatchWriteRow },
                { "/BatchGetRow",          DecodeBatchGetRow },
                { "/GetRange",             DecodeGetRange },

                 { "/ListSearchIndex",             DecodeListSearchIndex },
                 { "/CreateSearchIndex",             DecodeCreateSearchIndex },
                 { "/DescribeSearchIndex",             DecodeDescribeSearchIndex },
                 { "/DeleteSearchIndex",             DecodeDeleteSearchIndex },
                 { "/Search",             DecodeSearch },

                  { "/CreateIndex",             DecodeCreateGlobalIndex },
                  { "/DropIndex",             DecodeDeleteGlobalIndex },
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
            LogEncodedMessage(context, message);
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

        private Response.OTSResponse DecodeDeleteSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.DeleteSearchIndexResponse();
            var builder = PB.DeleteSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            _message = message;
            return response;
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

            return response;
        }

        private Response.OTSResponse DecodeDescribeSearchIndex(byte[] body, out IMessage _message)
        {
            var response = new Response.DescribeSearchIndexResponse();
            var builder = PB.DescribeSearchIndexResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            response.Schema = ParseIndexSchema(message.Schema);
            response.SyncStat = ParseSyncStat(message.SyncStat);
            _message = message;
            return response;
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
            ret.Analyzer = ParseAnalyzer(fieldSchema.Analyzer);
            ret.EnableSortAndAgg = fieldSchema.DocValues;
            ret.index = fieldSchema.Index;
            ret.Store = fieldSchema.Store;
            ret.IsArray = fieldSchema.IsArray;
            ret.IndexOptions = ParseIndexOption(fieldSchema.IndexOptions);
            foreach (var item in fieldSchema.FieldSchemasList)
            {
                ret.SubFieldSchemas.Add(ParseFieldSchema(item));
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
                default:
                    throw new OTSClientException(
                        String.Format("Invalid Analyzer type {0}", analyzer)
                    );
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

            var ret = new DataModel.TableMeta(
                tableMeta.TableName,
                schema
            );

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
                        String.Format("Invalid column type {0}", type)
                    );
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
    }
}
