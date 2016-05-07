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
using Model = Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Handler
{
    public class ProtoBufferEncoder : PipelineHandler
    {
        private delegate IMessage RequestEncoder(Request.OTSRequest request);
        private Dictionary<string, RequestEncoder> EncoderMap;
        
        public ProtoBufferEncoder(PipelineHandler innerHandler) : base(innerHandler) 
        {
            EncoderMap = new Dictionary<string, RequestEncoder>() {
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
        
        private PB.ColumnType MakeColumnType(Model.ColumnValueType type)
        {
            switch (type) 
            {
                case Model.ColumnValueType.Integer:
                    return PB.ColumnType.INTEGER;
                case Model.ColumnValueType.String:
                    return PB.ColumnType.STRING;
                case Model.ColumnValueType.Double:
                    return PB.ColumnType.DOUBLE;
                case Model.ColumnValueType.Boolean:
                    return PB.ColumnType.BOOLEAN;
                case Model.ColumnValueType.Binary:
                    return PB.ColumnType.BINARY;
                default:
                    throw new OTSClientException(String.Format(
                        "Invalid column value type: {0}", type
                    ));
            }
        }

        private PB.ComparatorType MakeComparatorType(RelationalCondition.CompareOperator oper)
        {
            switch (oper)
            {
                case RelationalCondition.CompareOperator.EQUAL:
                    return PB.ComparatorType.CT_EQUAL;
                case RelationalCondition.CompareOperator.NOT_EQUAL:
                    return PB.ComparatorType.CT_NOT_EQUAL;
                case RelationalCondition.CompareOperator.GREATER_THAN:
                    return PB.ComparatorType.CT_GREATER_THAN;
                case RelationalCondition.CompareOperator.GREATER_EQUAL:
                    return PB.ComparatorType.CT_GREATER_EQUAL;
                case RelationalCondition.CompareOperator.LESS_THAN:
                    return PB.ComparatorType.CT_LESS_THAN;
                case RelationalCondition.CompareOperator.LESS_EQUAL:
                    return PB.ComparatorType.CT_LESS_EQUAL;
                default:
                    throw new OTSClientException(String.Format("Invalid comparator type: {0}", oper));
            }
        }

        private PB.LogicalOperator MakeLogicOperator(CompositeCondition.LogicOperator type)
        {
            switch (type)
            {
                case CompositeCondition.LogicOperator.NOT:
                    return PB.LogicalOperator.LO_NOT;
                case CompositeCondition.LogicOperator.AND:
                    return PB.LogicalOperator.LO_AND;
                case CompositeCondition.LogicOperator.OR:
                    return PB.LogicalOperator.LO_OR;
                default:
                    throw new OTSClientException(String.Format("Invalid logic operator: {0}", type));
            }
        }

        private PB.ColumnConditionType MakeColumnConditionType(ColumnConditionType type)
        {
            switch (type)
            {
                case ColumnConditionType.COMPOSITE_CONDITION:
                    return PB.ColumnConditionType.CCT_COMPOSITE;
                case ColumnConditionType.RELATIONAL_CONDITION:
                    return PB.ColumnConditionType.CCT_RELATION;
                default:
                    throw new OTSClientException(String.Format("Invalid column condition type: {0}", type));
            }
        }
        
        private PB.ColumnSchema MakeColumnSchema(Tuple<string, Model.ColumnValueType> schema)
        {
            var builder = PB.ColumnSchema.CreateBuilder();
            builder.SetName(schema.Item1);
            builder.SetType(MakeColumnType(schema.Item2));
            return builder.Build();
        }
        
        private IEnumerable<PB.ColumnSchema> MakePrimaryKeySchema(DataModel.PrimaryKeySchema schema)
        {
            foreach (var item in schema)
            {
                yield return MakeColumnSchema(item);
            }
        }
        
        private PB.TableMeta MakeTableMeta(Model.TableMeta tableMeta)
        {
            var builder = PB.TableMeta.CreateBuilder();
            builder.SetTableName(tableMeta.TableName);
            builder.AddRangePrimaryKey(MakePrimaryKeySchema(tableMeta.PrimaryKeySchema));
            return builder.Build();
        }
        
        private PB.CapacityUnit MakeCapacityUnit(Model.CapacityUnit capacityUnit)
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

        private PB.ColumnCondition MakeColumnCondition(ColumnCondition cc)
        {
            PB.ColumnCondition.Builder builder = PB.ColumnCondition.CreateBuilder();
            builder.SetType(MakeColumnConditionType(cc.GetType()));
            if (cc.GetType() == ColumnConditionType.COMPOSITE_CONDITION){
                builder.SetCondition(BuildCompositeCondition((CompositeCondition)cc));
            } else if (cc.GetType() == ColumnConditionType.RELATIONAL_CONDITION) {
                builder.SetCondition(BuildRelationalCondition((RelationalCondition)cc));
            } else {
                throw new OTSClientException(String.Format("Invalid column condition type: {0}", cc.GetType()));
            }
            return builder.Build();
        }

        private ByteString BuildCompositeCondition(CompositeCondition cc)
        {
            PB.CompositeCondition.Builder builder = PB.CompositeCondition.CreateBuilder();
            builder.SetCombinator(MakeLogicOperator(cc.Type));
            foreach (ColumnCondition c in cc.SubConditions)
            {
                builder.AddSubConditions(MakeColumnCondition(c));
            }
            return builder.Build().ToByteString();
        }

        private ByteString BuildRelationalCondition(RelationalCondition scc)
        {
            PB.RelationCondition.Builder builder = PB.RelationCondition.CreateBuilder();
            builder.SetColumnName(scc.ColumnName);
            builder.SetComparator(MakeComparatorType(scc.Operator));
            builder.SetColumnValue(MakeColumnValue(scc.ColumnValue));
            builder.SetPassIfMissing(scc.PassIfMissing);
            return builder.Build().ToByteString();
        }
        
        private PB.ReservedThroughput MakeReservedThroughput(Model.CapacityUnit reservedThroughput)
        {
            var builder = PB.ReservedThroughput.CreateBuilder();
            builder.SetCapacityUnit(MakeCapacityUnit(reservedThroughput));
            return builder.Build();
        }
        
        private IMessage EncodeCreateTable(Request.OTSRequest request)
        {
            var requestReal = (Request.CreateTableRequest)request;
            var builder = PB.CreateTableRequest.CreateBuilder();
            builder.SetTableMeta(MakeTableMeta(requestReal.TableMeta));
            builder.SetReservedThroughput(MakeReservedThroughput(requestReal.ReservedThroughput));
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
            builder.SetReservedThroughput(MakeReservedThroughput(requestReal.ReservedThroughput));
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

        private PB.Condition MakeCondition(Model.Condition condition)
        {
            PB.Condition.Builder builder = PB.Condition.CreateBuilder();
            if (condition.RowExistenceExpect == Model.RowExistenceExpectation.EXPECT_EXIST) {
                builder.SetRowExistence(PB.RowExistenceExpectation.EXPECT_EXIST);
            } else if (condition.RowExistenceExpect == Model.RowExistenceExpectation.EXPECT_NOT_EXIST){
                builder.SetRowExistence(PB.RowExistenceExpectation.EXPECT_NOT_EXIST);
            } else if (condition.RowExistenceExpect == Model.RowExistenceExpectation.IGNORE) {
                builder.SetRowExistence(PB.RowExistenceExpectation.IGNORE);
            } else {
                throw new OTSClientException(String.Format("Invalid RowExistenceExpectation: {0}", condition.RowExistenceExpect));
            }
            if (condition.ColumnCondition != null)
            {
                builder.SetColumnCondition(MakeColumnCondition(condition.ColumnCondition));
            }
            return builder.Build();
        }
        
        private PB.ColumnValue MakeColumnValue(DataModel.ColumnValue value)
        {
            var builder = PB.ColumnValue.CreateBuilder();
            
            if (value == DataModel.ColumnValue.INF_MAX)
            {
                builder.SetType(PB.ColumnType.INF_MAX);
            }
            else if (value == DataModel.ColumnValue.INF_MIN)
            {
                builder.SetType(PB.ColumnType.INF_MIN);
            }
            else {
                switch (value.Type)
                {
                    case DataModel.ColumnValueType.Binary:
                        builder.SetType(PB.ColumnType.BINARY);
                        builder.SetVBinary(ByteString.CopyFrom(value.BinaryValue));
                        break;
                        
                    case DataModel.ColumnValueType.String:
                        builder.SetType(PB.ColumnType.STRING);
                        builder.SetVString(value.StringValue);
                        break;
                        
                    case DataModel.ColumnValueType.Boolean:
                        builder.SetType(PB.ColumnType.BOOLEAN);
                        builder.SetVBool(value.BooleanValue);
                        break;
                        
                    case DataModel.ColumnValueType.Double:
                        builder.SetType(PB.ColumnType.DOUBLE);
                        builder.SetVDouble(value.DoubleValue);
                        break;
                        
                    case DataModel.ColumnValueType.Integer:
                        builder.SetType(PB.ColumnType.INTEGER);
                        builder.SetVInt(value.IntegerValue);
                        break;
                        
                    default:
                        throw new OTSClientException(
                            String.Format("Invalid column value type: {0}", value.Type)
                        );
                }
            }
                
            return builder.Build();
        }
        
        private PB.Column MakeColumn(string name, DataModel.ColumnValue value)
        {
            var builder = PB.Column.CreateBuilder();
            builder.SetName(name);
            builder.SetValue(MakeColumnValue(value));
            return builder.Build();
        }
        
        private IEnumerable<PB.Column> MakeColumns(Dictionary<string, DataModel.ColumnValue> columns)
        {
            foreach (var column in columns)
            {
                yield return MakeColumn(column.Key, column.Value);
            }
        }
        
        private IMessage EncodePutRow(Request.OTSRequest request)
        {
            var requestReal = (Request.PutRowRequest)request;
            var builder = PB.PutRowRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetCondition(MakeCondition(requestReal.Condition));
            builder.AddRangePrimaryKey(MakeColumns(requestReal.PrimaryKey));
            builder.AddRangeAttributeColumns(MakeColumns(requestReal.Attribute));
            return builder.Build();
        }
        
        private IMessage EncodeGetRow(Request.OTSRequest request)
        {            
            var requestReal = (Request.GetRowRequest)request;
            var builder = PB.GetRowRequest.CreateBuilder();
            builder.SetTableName(requestReal.QueryCriteria.TableName);
            builder.AddRangePrimaryKey(MakeColumns(requestReal.QueryCriteria.RowPrimaryKey));
            builder.AddRangeColumnsToGet(requestReal.QueryCriteria.GetColumnsToGet());
            if (requestReal.QueryCriteria.Filter != null)
            {
                builder.SetFilter(MakeColumnCondition(requestReal.QueryCriteria.Filter));
            }

            return builder.Build();
        }
        
        private PB.ColumnUpdate MakeColumnUpdateForDelete(string columnName)
        {
            var builder = PB.ColumnUpdate.CreateBuilder();
            builder.SetType(PB.OperationType.DELETE);
            builder.SetName(columnName);
            return builder.Build();
        }
        
        private PB.ColumnUpdate MakeColumnUpdateForPut(string columnName, DataModel.ColumnValue value)
        {
            var builder = PB.ColumnUpdate.CreateBuilder();
            builder.SetType(PB.OperationType.PUT);
            builder.SetName(columnName);
            builder.SetValue(MakeColumnValue(value));
            return builder.Build();
        }
        
        private IEnumerable<PB.ColumnUpdate> MakeUpdateOfAttribute(DataModel.UpdateOfAttribute update)
        {
            foreach (var item in update.AttributeColumnsToPut)
            {
                yield return MakeColumnUpdateForPut(item.Key, item.Value);
            }
            
            foreach (var item in update.AttributeColumnsToDelete)
            {
                yield return MakeColumnUpdateForDelete(item);
            }
        }
        
        private IMessage EncodeUpdateRow(Request.OTSRequest request)
        {
            var requestReal = (Request.UpdateRowRequest)request;
            var builder = PB.UpdateRowRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetCondition(MakeCondition(requestReal.Condition));
            builder.AddRangePrimaryKey(MakeColumns(requestReal.PrimaryKey));
            builder.AddRangeAttributeColumns(MakeUpdateOfAttribute(requestReal.UpdateOfAttribute));
            
            return builder.Build();
        }
        
        private IMessage EncodeDeleteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.DeleteRowRequest)request;
            var builder = PB.DeleteRowRequest.CreateBuilder();
            builder.SetTableName(requestReal.TableName);
            builder.SetCondition(MakeCondition(requestReal.Condition));
            builder.AddRangePrimaryKey(MakeColumns(requestReal.PrimaryKey));
            return builder.Build();
        }
        
        private PB.TableInBatchWriteRowRequest 
            MakeTableInBatchWriteRowRequest(string tableName, DataModel.RowChanges rowChanges)
        {
            var builder = PB.TableInBatchWriteRowRequest.CreateBuilder();
            
            builder.SetTableName(tableName);
            
            foreach (var op in rowChanges.PutOperations)
            {
                var putBuilder = PB.PutRowInBatchWriteRowRequest.CreateBuilder();
                putBuilder.SetCondition(MakeCondition(op.Item1));
                putBuilder.AddRangePrimaryKey(MakeColumns(op.Item2));
                putBuilder.AddRangeAttributeColumns(MakeColumns(op.Item3));
                builder.AddPutRows(putBuilder.Build());
            }
            
            foreach (var op in rowChanges.UpdateOperations)
            {
                var updateBuilder = PB.UpdateRowInBatchWriteRowRequest.CreateBuilder();
                updateBuilder.SetCondition(MakeCondition(op.Item1));
                updateBuilder.AddRangePrimaryKey(MakeColumns(op.Item2));
                updateBuilder.AddRangeAttributeColumns(MakeUpdateOfAttribute(op.Item3));
                builder.AddUpdateRows(updateBuilder.Build());
            }
            
            foreach (var op in rowChanges.DeleteOperations)
            {
                var deleteBuilder = PB.DeleteRowInBatchWriteRowRequest.CreateBuilder();
                deleteBuilder.SetCondition(MakeCondition(op.Item1));
                deleteBuilder.AddRangePrimaryKey(MakeColumns(op.Item2));
                builder.AddDeleteRows(deleteBuilder.Build());
            }
            
            return builder.Build();
        }
        
        private IMessage EncodeBatchWriteRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchWriteRowRequest)request;
            var builder = PB.BatchWriteRowRequest.CreateBuilder();
            
            foreach (var item in requestReal.RowChangesGroupByTable) {
                builder.AddTables(MakeTableInBatchWriteRowRequest(item.Key, item.Value));
            }
            
            return builder.Build();
        }
        
        private PB.TableInBatchGetRowRequest 
            MakeTableInBatchGetRowRequest(Model.MultiRowQueryCriteria criteria)
        {
            var builder = PB.TableInBatchGetRowRequest.CreateBuilder();
            builder.SetTableName(criteria.TableName);
            
            foreach (var primaryKey in criteria.GetRowKeys())
            {
                var rowBuilder = PB.RowInBatchGetRowRequest.CreateBuilder();
                rowBuilder.AddRangePrimaryKey(MakeColumns(primaryKey));
                builder.AddRows(rowBuilder.Build());
            }
            
            if (criteria.GetColumnsToGet() != null) {
                builder.AddRangeColumnsToGet(criteria.GetColumnsToGet());
            }

            if (criteria.Filter != null)
            {
                builder.SetFilter(MakeColumnCondition(criteria.Filter));
            }
            return builder.Build();
        }
        
        private IMessage EncodeBatchGetRow(Request.OTSRequest request)
        {
            var requestReal = (Request.BatchGetRowRequest)request;
            var builder = PB.BatchGetRowRequest.CreateBuilder();
            
            foreach (var criterias in requestReal.GetCriterias())
            {
                builder.AddTables(MakeTableInBatchGetRowRequest(criterias));
            }
            
            return builder.Build();
        }
        
        private PB.Direction MakeDirection(Request.GetRangeDirection direction)
        {
            switch (direction)
            {
                case Request.GetRangeDirection.Forward:
                    return PB.Direction.FORWARD;
                case Request.GetRangeDirection.Backward:
                    return PB.Direction.BACKWARD;
                    
                default:
                    throw new OTSClientException(
                        String.Format("Invalid direction: {0}", direction)
                    );
            }
        }
        
        private IMessage EncodeGetRange(Request.OTSRequest request)
        {
            var requestReal = (Request.GetRangeRequest)request;
            var builder = PB.GetRangeRequest.CreateBuilder();
            builder.SetTableName(requestReal.QueryCriteria.TableName);
            builder.SetDirection(MakeDirection(requestReal.QueryCriteria.Direction));
            
            if (requestReal.QueryCriteria.GetColumnsToGet() != null) {
                builder.AddRangeColumnsToGet(requestReal.QueryCriteria.GetColumnsToGet());
            }
            
            if (requestReal.QueryCriteria.Limit != null) 
            {
                builder.SetLimit((int)requestReal.QueryCriteria.Limit);
            }

            if (requestReal.QueryCriteria.Filter != null)
            {
                builder.SetFilter(MakeColumnCondition(requestReal.QueryCriteria.Filter));
            }
            
            builder.AddRangeInclusiveStartPrimaryKey(
                MakeColumns(requestReal.QueryCriteria.InclusiveStartPrimaryKey));
            builder.AddRangeExclusiveEndPrimaryKey(
                MakeColumns(requestReal.QueryCriteria.ExclusiveEndPrimaryKey));
            
            return builder.Build();
        }
        
        private void LogEncodedMessage(Context context, IMessage message)
        {
            if (context.ClientConfig.OTSDebugLogHandler != null) {
                var msgString = String.Format("OTS Request API: {0} Protobuf: {1}\n",
                                    context.APIName,
                                    TextFormat.PrintToString(message));
                context.ClientConfig.OTSDebugLogHandler(msgString);
            }
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
    }
}
