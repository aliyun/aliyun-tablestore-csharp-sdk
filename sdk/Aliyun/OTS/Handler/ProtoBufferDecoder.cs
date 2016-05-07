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

using Google.ProtocolBuffers;

using PB = com.aliyun.cloudservice.ots2;

namespace Aliyun.OTS.Handler
{
    public class ProtoBufferDecoder : PipelineHandler
    {
        private delegate Response.OTSResponse ResponseDecoder(byte[] body, out IMessage message);
        private Dictionary<string, ResponseDecoder> DecoderMap;
        
        public ProtoBufferDecoder(PipelineHandler innerHandler) : base(innerHandler)
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
        
        private DataModel.CapacityUnit ParseCapacityUnit(PB.CapacityUnit capacityUnit)
        {
            return new DataModel.CapacityUnit(capacityUnit.Read, capacityUnit.Write);
        }
        
        private DataModel.ReservedThroughputDetails ParseReservedThroughputDetails(PB.ReservedThroughputDetails details)
        {
            var ret = new DataModel.ReservedThroughputDetails(
                ParseCapacityUnit(details.CapacityUnit),
                details.LastIncreaseTime,
                details.LastDecreaseTime,
                details.NumberOfDecreasesToday
            );
            
            return ret;
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
        
        private DataModel.ColumnValueType ParseColumnValueType(PB.ColumnType type)
        {
            switch (type)
            {
                case PB.ColumnType.BINARY:
                    return DataModel.ColumnValueType.Binary;
                case PB.ColumnType.BOOLEAN:
                    return DataModel.ColumnValueType.Boolean;
                case PB.ColumnType.DOUBLE:
                    return DataModel.ColumnValueType.Double;
                case PB.ColumnType.INTEGER:
                    return DataModel.ColumnValueType.Integer;
                case PB.ColumnType.STRING:
                    return DataModel.ColumnValueType.String;
                    
                default:
                    throw new OTSClientException(
                        String.Format("Invalid column type {0}", type)
                    );
            }
        }
        
        private DataModel.TableMeta ParseTableMeta(PB.TableMeta tableMeta)
        {
            var schema = new DataModel.PrimaryKeySchema();
            
            for (int i = 0; i < tableMeta.PrimaryKeyCount; i ++)
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
        
        private Response.OTSResponse DecodeDescribeTable(byte[] body, out IMessage _message)
        {
            var response = new Response.DescribeTableResponse();
            var builder = PB.DescribeTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            response.TableMeta = ParseTableMeta(message.TableMeta);
            response.ReservedThroughputDetails = ParseReservedThroughputDetails(message.ReservedThroughputDetails);
            _message = message;
            return response;
        }
        
        private Response.OTSResponse DecodeListTable(byte[] body, out IMessage _message)
        {
            var response = new Response.ListTableResponse();
            response.TableNames = new List<string>();
            
            var builder = PB.ListTableResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            
            for (int i = 0; i < message.TableNamesCount; i ++)
            {
                response.TableNames.Add(message.GetTableNames(i));
            }
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
        
        private DataModel.ColumnValue ParseColumnValue(PB.ColumnValue value)
        {
            switch (value.Type)
            {
                case PB.ColumnType.BINARY:
                    return new DataModel.ColumnValue(value.VBinary.ToByteArray());
                case PB.ColumnType.BOOLEAN:
                    return new DataModel.ColumnValue(value.VBool);
                case PB.ColumnType.DOUBLE:
                    return new DataModel.ColumnValue(value.VDouble);
                case PB.ColumnType.INTEGER:
                    return new DataModel.ColumnValue(value.VInt);
                case PB.ColumnType.STRING:
                    return new DataModel.ColumnValue(value.VString);
                    
                default:
                    throw new OTSClientException(
                        String.Format("Invalid column type {0}", value.Type)
                    );
            }
        }
        
        private DataModel.PrimaryKey ParsePrimaryKey(IList<PB.Column> columns)
        {
            var ret = new DataModel.PrimaryKey();
            
            foreach (var column in columns)
            {
                ret.Add(column.Name, ParseColumnValue(column.Value));
            }
            
            return ret;
        }
        
        private DataModel.AttributeColumns ParseAttribute(IList<PB.Column> columns)
        {
            var ret = new DataModel.AttributeColumns();
            
            foreach (var column in columns)
            {
                ret.Add(column.Name, ParseColumnValue(column.Value));
            }
            
            return ret;
        }
        
        private Response.OTSResponse DecodeGetRow(byte[] body, out IMessage _message)
        {            
            var builder = PB.GetRowResponse.CreateBuilder();
            builder.MergeFrom(body);
            var message = builder.Build();
            var primaryKey = ParsePrimaryKey(message.Row.PrimaryKeyColumnsList);
            var attribute = ParseAttribute(message.Row.AttributeColumnsList);
            
            var response = new Response.GetRowResponse(
                ParseCapacityUnit(message.Consumed.CapacityUnit),
                primaryKey,
                attribute
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
        
        private IList<Response.BatchWriteRowResponseItem>
            ParseBatchWriteRowResponseItems(IList<PB.RowInBatchWriteRowResponse> responseItems)
        {
            var ret = new List<Response.BatchWriteRowResponseItem>();
            foreach (var responseItem in responseItems)
            {
                if (responseItem.IsOk)
                {
                    ret.Add(new Response.BatchWriteRowResponseItem(
                        ParseCapacityUnit(responseItem.Consumed.CapacityUnit)));                    
                }
                else
                {
                    ret.Add(new Response.BatchWriteRowResponseItem(
                        responseItem.Error.Code, responseItem.Error.Message
                   ));
                }
            }
            
            return ret;
        }
        
        private Response.BatchWriteRowResponseForOneTable 
            ParseTableInBatchWriteRowResponse(PB.TableInBatchWriteRowResponse table)
        {
            var ret = new Response.BatchWriteRowResponseForOneTable();
            ret.PutResponses = ParseBatchWriteRowResponseItems(table.PutRowsList);
            ret.DeleteResponses = ParseBatchWriteRowResponseItems(table.DeleteRowsList);
            ret.UpdateResponses = ParseBatchWriteRowResponseItems(table.UpdateRowsList);
            return ret;
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
        
        private IList<Response.BatchGetRowResponseItem>
            ParseTableInBatchGetRowResponse(PB.TableInBatchGetRowResponse table)
        {
            var ret = new List<Response.BatchGetRowResponseItem>();
            
            foreach (var row in table.RowsList)
            {
                if (row.IsOk)
                {
                    ret.Add(new Response.BatchGetRowResponseItem(
                        ParseCapacityUnit(row.Consumed.CapacityUnit),
                        ParsePrimaryKey(row.Row.PrimaryKeyColumnsList),
                        ParseAttribute(row.Row.AttributeColumnsList)
                    ));
                }
                else
                {
                    ret.Add(new Response.BatchGetRowResponseItem(
                        row.Error.Code, row.Error.Message));
                }
            }
            
            return ret;
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
            var response = new Response.GetRangeResponse();
            
            response.ConsumedCapacityUnit = ParseCapacityUnit(message.Consumed.CapacityUnit);
            response.NextPrimaryKey = ParsePrimaryKey(message.NextStartPrimaryKeyList);
            
            if (response.NextPrimaryKey.Count == 0)
            {
                // No next PK returned
                response.NextPrimaryKey = null;
            }
            
            foreach (var row in message.RowsList)
            {
                var rowData = new Response.RowDataFromGetRange();
                rowData.PrimaryKey = ParsePrimaryKey(row.PrimaryKeyColumnsList);
                rowData.Attribute = ParseAttribute(row.AttributeColumnsList);
                response.RowDataList.Add(rowData);
            }
            _message = message;
            return response;
        }
        
        public override void HandleBefore(Context context)
        {
            InnerHandler.HandleBefore(context);
        }
        
        private void LogEncodedMessage(Context context, IMessage message)
        {
            if (context.ClientConfig.OTSDebugLogHandler != null) {
                string requestID = "";
                if (context.HttpResponseHeaders.ContainsKey("x-ots-requestid")) {
                    requestID = context.HttpResponseHeaders["x-ots-requestid"];
                }
                var msgString = String.Format("OTS Response API: {0} RequestID: {1} Protobuf: {2}\n", 
                                              context.APIName,
                                              requestID,
                                              TextFormat.PrintToString(message));
                
                context.ClientConfig.OTSDebugLogHandler(msgString);
            }
        }

        public override void HandleAfter(Context context) 
        {
            InnerHandler.HandleAfter(context);
            IMessage message;
            context.OTSReponse = DecoderMap[context.APIName](context.HttpResponseBody, out message);
            LogEncodedMessage(context, message);
        }
    }
}
