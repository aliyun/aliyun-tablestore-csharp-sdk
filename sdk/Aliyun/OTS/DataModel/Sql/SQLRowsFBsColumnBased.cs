using com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers;
using System;
using System.Collections.Generic;
using System.IO;

namespace Aliyun.OTS.DataModel.SQL
{
    public class SQLRowsFBsColumnBased : ISQLRows
    {
        public SQLTableMeta SQLTableMeta { get; set; }

        public string[] ColumnNames { get; set; }

        public byte[] ColumnTypes { get; set; }

        public ColumnValues[] ColumnValues { get; set; }

        public RLEStringValues[] RLEStringValues { get; set; }

        public long RowCount { get; set; }

        public long ColumnCount { get; set; }

        public SQLRowsFBsColumnBased(SQLResponseColumns columns)
        {
            ColumnNames = new string[columns.ColumnsLength];
            ColumnTypes = new byte[columns.ColumnsLength];
            ColumnValues = new ColumnValues[columns.ColumnsLength];
            RLEStringValues = new RLEStringValues[columns.ColumnsLength];

            for (int i = 0; i < columns.ColumnsLength; i++)
            {
                if (!columns.Columns(i).HasValue)
                {
                    continue;
                }

                SQLResponseColumn column = (SQLResponseColumn)columns.Columns(i);
                ColumnNames[i] = column.ColumnName;
                ColumnTypes[i] = (byte)column.ColumnType;

                if (!column.ColumnValue.HasValue)
                {
                    continue;
                }

                ColumnValues[i] = (ColumnValues)column.ColumnValue;

                if (ColumnValues[i].RleStringValues != null)
                {
                    RLEStringValues[i] = (RLEStringValues)(ColumnValues[i].RleStringValues);
                }
            }

            RowCount = columns.RowCount;

            ColumnCount = columns.ColumnsLength;

            SQLTableMeta = ResolveSQLTableMetaFromColumns();
        }

        public SQLTableMeta GetSQLTableMeta()
        {
            return SQLTableMeta;
        }

        public long GetRowCount()
        {
            return RowCount;
        }

        public long GetColumnCount()
        {
            return ColumnCount;
        }

        public Object GetObject(int rowIndex, int columnIndex)
        {
            if (rowIndex >= RowCount || rowIndex < 0)
            {
                throw new ArgumentOutOfRangeException(string.Format("Row index {0} out of range!", rowIndex));
            }

            if (columnIndex >= ColumnCount || columnIndex < 0)
            {
                throw new ArgumentOutOfRangeException(string.Format("Column index {0} out of range!", columnIndex));
            }

            byte columnType = ColumnTypes[columnIndex];

            ColumnValues columnValue = ColumnValues[columnIndex];

            switch ((DataType)columnType)
            {
                case DataType.LONG:
                    return columnValue.LongValues(rowIndex);
                case DataType.BOOLEAN:
                    return columnValue.BoolValues(rowIndex);
                case DataType.DOUBLE:
                    return columnValue.DoubleValues(rowIndex);
                case DataType.STRING:
                    return columnValue.StringValues(rowIndex);
                case DataType.BINARY:
                    BytesValue bytesValue = (BytesValue)columnValue.BinaryValues(rowIndex);
                    sbyte[] sbytes = bytesValue.GetValueArray();
                    byte[] bytes = new byte[sbytes.Length];
                    Buffer.BlockCopy(sbytes, 0, bytes, 0, sbytes.Length);
                    return new MemoryStream(bytes);
                case DataType.STRING_RLE:
                    RLEStringValues rleStringValue = RLEStringValues[columnIndex];
                    return ResolveRLEString(rleStringValue, rowIndex);
                default:
                    throw new NotSupportedException(string.Format("not support column type in flatbuffers: {0}", columnType));
            }
        }

        private string ResolveRLEString(RLEStringValues rleStringValue, int rowIndex)
        {
            return rleStringValue.Array(rleStringValue.IndexMapping(rowIndex));
        }


        private SQLTableMeta ResolveSQLTableMetaFromColumns()
        {
            List<SQLColumnSchema> schemas = new List<SQLColumnSchema>();
            Dictionary<string, int?> columnsMap = new Dictionary<string, int?>();

            for (int i = 0; i < ColumnCount; i++)
            {
                schemas.Add(new SQLColumnSchema(ColumnNames[i], ConvertColumnType(ColumnTypes[i])));
                columnsMap.Add(ColumnNames[i], i);
            }

            return new SQLTableMeta(schemas, columnsMap);
        }

        private ColumnValueType ConvertColumnType(byte columnType)
        {
            switch ((DataType)columnType)
            {
                case DataType.LONG:
                    return ColumnValueType.Integer;
                case DataType.BOOLEAN:
                    return ColumnValueType.Boolean;
                case DataType.DOUBLE:
                    return ColumnValueType.Double;
                case DataType.STRING:
                case DataType.STRING_RLE:
                    return ColumnValueType.String;
                case DataType.BINARY:
                    return ColumnValueType.Binary;
                default:
                    throw new ArgumentException(string.Format("not support column type in flatbuffers: {0}", columnType));
            }
        }
    }
}
