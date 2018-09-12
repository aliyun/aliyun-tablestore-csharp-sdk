using System;
using System.Collections.Generic;
using Google.ProtocolBuffers;
using System.IO;
using PB = com.alicloud.openservices.tablestore.core.protocol;

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

            var response = new Response.PutRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit)
            );
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeGetRow(byte[] body, out IMessage _message)
        {
            var builder = PB.GetRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            Console.WriteLine(message.ToJson());

            DataModel.Row row = null;

            if (message.HasRow && !message.Row.IsEmpty)
            {
                PB.PlainBufferCodedInputStream inputStream = new PB.PlainBufferCodedInputStream(message.Row.CreateCodedInput());
                List<PB.PlainBufferRow> rows = inputStream.ReadRowsWithHeader();
                if (rows.Count != 1)
                {
                    throw new IOException("Expect only returns one row. Row count: " + rows.Count);
                }

                row = PB.PlainBufferConversion.ToRow(rows[0]) as DataModel.Row;
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

            var response = new Response.UpdateRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit)
            );
            _message = message;
            return response;
        }

        private Response.OTSResponse DecodeDeleteRow(byte[] body, out IMessage _message)
        {
            var builder = PB.DeleteRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();

            var response = new Response.DeleteRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit)
            );
            _message = message;
            return response;
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
    }
}
