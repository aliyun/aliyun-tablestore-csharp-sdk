using System.IO;
using Aliyun.OTS.DataModel;
using System.Collections.Generic;
using System;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public static class PlainBufferBuilder
    {
        public static int ComputePrimaryKeyValue(ColumnValue value)
        {
            int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
            size += ComputePrimaryKeyValueWithoutLengthPrefix(value);
            return size;
        }


        // Bytes Data: value_type + type
        public static int ComputePrimaryKeyValueWithoutLengthPrefix(ColumnValue value)
        {
            int size = 1; // length + type + value
            if (value.IsInfMin() || value.IsInfMax() || value.IsPlaceHolderForAutoIncr())
            {
                return size; // inf value and AutoIncr only has a type.
            }

            switch (value.Type)
            {
                case ColumnValueType.String:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                        size += value.AsStringInBytes().Length;
                        break;
                    }
                case ColumnValueType.Integer:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                        break;
                    }
                case ColumnValueType.Binary:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                        size += value.BinaryValue.Length;
                        break;
                    }
                default:
                    throw new IOException("Bug: unsupported primary key type: " + value.Type);

            }
            return size;
        }

        public static int ComputeColumnValue(ColumnValue value)
        {
            int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
            size += ComputeColumnValueWithoutLengthPrefix(value);
            return size;
        }

        // Bytes Data: value_type + type
        public static int ComputeColumnValueWithoutLengthPrefix(ColumnValue value)
        {
            int size = 1; // length + type + value

            if (value.IsInfMin() || value.IsInfMax() || value.IsPlaceHolderForAutoIncr())
            {
                return size; // inf value and AutoIncr only has a type.
            }

            switch (value.Type)
            {
                case ColumnValueType.String:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                        size += value.AsStringInBytes().Length;
                        break;
                    }
                case ColumnValueType.Integer:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                        break;
                    }
                case ColumnValueType.Binary:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                        size += value.BinaryValue.Length;
                        break;
                    }
                case ColumnValueType.Double:
                    {
                        size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                        break;
                    }
                case ColumnValueType.Boolean:
                    {
                        size += 1;
                        break;
                    }
                default:
                    throw new IOException("Bug: unsupported column type: " + value.Type);

            }

            return size;
        }

        public static int ComputePlainBufferExtension(PlainBufferExtension extension)
        {
            int size = 1; //TAG_EXTENSION
            size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Length
            if (extension.HasSeq())
            {
                size += ComputePlainBufferSequenceInfo();
            }

            return size;
        }

        public static int ComputePlainBufferSequenceInfo()
        {
            int size = 1;//TAG_SEQ_INFO
            size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Length
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_EPOCH + epoch
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_SEQ_INFO_TS + timestamp
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_ROW_INDEX + rowIndex
            return size;
        }

        public static int ComputePlainBufferCell(PlainBufferCell cell)
        {
            int size = 1; // TAG_CELL
            if (cell.HasCellName())
            {
                size += 1; // TAG_CELL_NAME
                size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // length
                size += cell.GetNameRawData().Length;
            }
            if (cell.HasCellValue())
            {
                size += 1; // TAG_CELL_VALUE
                if (cell.IsPk())
                {
                    size += ComputePrimaryKeyValue(cell.GetPkCellValue());
                }
                else
                {
                    size += ComputeColumnValue(cell.GetCellValue());
                }
            }
            if (cell.HasCellType())
            {
                size += 2; // TAG_CELL_OP_TYPE + type
            }
            if (cell.HasCellTimestamp())
            {
                size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_CELL_TIMESTAMP + timestamp
            }
            size += 2; // TAG_CELL_CHECKSUM + checksum
            return size;
        }

        public static int ComputePlainBufferRow(PlainBufferRow row)
        {
            int size = 0;
            size += 1; // TAG_ROW_PK
            foreach (PlainBufferCell cell in row.GetPrimaryKey())
            {
                size += ComputePlainBufferCell(cell);
            }
            if (row.GetCells().Count > 0)
            {
                size += 1; // TAG_ROW_DATA
                foreach (PlainBufferCell cell in row.GetCells())
                {
                    size += ComputePlainBufferCell(cell);
                }
            }
            if (row.HasDeleteMarker())
            {
                size += 1; // TAG_DELETE_MARKER
            }
            if (row.HasExtension())
            {
                size += ComputePlainBufferExtension(row.GetExtension());
            }
            size += 2; // TAG_ROW_CHECKSUM + checksum
            return size;
        }

        public static int ComputePlainBufferRowWithHeader(PlainBufferRow row)
        {
            int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // header
            size += ComputePlainBufferRow(row);
            return size;
        }


        public static int ComputeSkipLengthForExtensionTag(PlainBufferExtension extension)
        {
            int size = 0;
            if (extension.HasSeq())
            {
                size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; //TAG_SEQ_ING + length
                size += ComputeSkipLengthForSequenceInfo();
            }

            return size;
        }

        public static int ComputeSkipLengthForSequenceInfo()
        {
            int size = 0;
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_EPOCH + epoch
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_SEQ_INFO_TS + timestamp
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_ROW_INDEX + rowIndex
            return size;
        }

        public static byte[] BuildPrimaryKeyValueWithoutLengthPrefix(ColumnValue value)
        {
            int size = ComputePrimaryKeyValueWithoutLengthPrefix(value);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
            codedOutput.WritePrimaryKeyValueWithoutLengthPrefix(value);

            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize primary key value failed. Buffer remains " + output.Remain());
            }

            return output.GetBuffer();
        }

        public static byte[] BuildColumnValueWithoutLengthPrefix(ColumnValue value)
        {
            int size = ComputeColumnValueWithoutLengthPrefix(value);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
            codedOutput.WriteColumnValueWithoutLengthPrefix(value);

            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize column value failed. Buffer remains " + output.Remain());
            }

            return output.GetBuffer();
        }

        public static int ComputePrimaryKeyColumn(Column column)
        {
            int size = 2; // TAG_CELL + TAG_CELL_NAME;
            size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
            size += column.GetNameRawData().Length;
            size += 1; // TAG_CELL_VALUE
            size += ComputePrimaryKeyValue(column.Value);
            size += 2; // TAG_CELL_CHECKSUM + checksum
            return size;
        }

        // Bytes Data: TAG_ROW_PK + [primary key columns]
        public static int ComputePrimaryKey(PrimaryKey primaryKey)
        {
            int size = 1; // TAG_ROW_PK
            foreach (var key in primaryKey)
            {
                var column = new Column(key.Key, key.Value);
                size += ComputePrimaryKeyColumn(column);
            }

            return size;
        }

        public static int ComputePrimaryKeyWithHeader(PrimaryKey primaryKey)
        {
            int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Header
            size += ComputePrimaryKey(primaryKey);
            size += 2; // TAG_ROW_CHECKSUM + checksum
            return size;
        }

        public static void WritePrimaryKeyValue(ColumnValue value, PlainBufferOutputStream output)
        {
            if (value.IsInfMin())
            {
                output.WriteRawLittleEndian32(1);
                output.WriteRawByte(PlainBufferConsts.VT_INF_MIN);
                return;
            }

            if (value.IsInfMax())
            {
                output.WriteRawLittleEndian32(1);
                output.WriteRawByte(PlainBufferConsts.VT_INF_MAX);
                return;
            }

            if (value.IsPlaceHolderForAutoIncr())
            {
                output.WriteRawLittleEndian32(1);
                output.WriteRawByte(PlainBufferConsts.VT_AUTO_INCREMENT);
                return;
            }

            switch (value.Type)
            {
                case ColumnValueType.String:
                    {
                        byte[] rawData = value.AsStringInBytes();
                        int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                        output.WriteRawLittleEndian32(prefixLength + rawData.Length); // length + type + value
                        output.WriteRawByte(PlainBufferConsts.VT_STRING);
                        output.WriteRawLittleEndian32(rawData.Length);
                        output.WriteBytes(rawData);
                        break;
                    }
                case ColumnValueType.Integer:
                    {
                        output.WriteRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                        output.WriteRawByte(PlainBufferConsts.VT_INTEGER);
                        output.WriteRawLittleEndian64(value.IntegerValue);
                        break;
                    }
                case ColumnValueType.Binary:
                    {
                        byte[] rawData = value.BinaryValue;
                        int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                        output.WriteRawLittleEndian32(prefixLength + rawData.Length); // length + type + value
                        output.WriteRawByte(PlainBufferConsts.VT_BLOB);
                        output.WriteRawLittleEndian32(rawData.Length);
                        output.WriteBytes(rawData);
                        break;
                    }
                default:
                    throw new IOException("Bug: unsupported primary key type: " + value.GetType());
            }
        }

        public static void WritePrimaryKeyColumn(Column column, PlainBufferOutputStream output, byte checksum)
        {
            output.WriteRawByte(PlainBufferConsts.TAG_CELL);
            output.WriteRawByte(PlainBufferConsts.TAG_CELL_NAME);
            byte[] rawData = column.GetNameRawData();
            output.WriteRawLittleEndian32(rawData.Length);
            output.WriteBytes(rawData);
            output.WriteRawByte(PlainBufferConsts.TAG_CELL_VALUE);
            WritePrimaryKeyValue(column.Value, output);
            output.WriteRawByte(PlainBufferConsts.TAG_CELL_CHECKSUM);
            output.WriteRawByte(checksum);
        }

        public static byte[] BuildPrimaryKeyWithHeader(PrimaryKey primaryKey)
        {
            int size = ComputePrimaryKeyWithHeader(primaryKey);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            output.WriteRawLittleEndian32(PlainBufferConsts.HEADER);
            output.WriteRawByte(PlainBufferConsts.TAG_ROW_PK);

            byte rowChecksum = (byte)0x0, cellChecksum;

            foreach (var key in primaryKey)
            {
                var column = new Column(key.Key, key.Value);
                cellChecksum = PlainBufferCrc8.crc8((byte)0x0, column.GetNameRawData());
                cellChecksum = column.Value.GetChecksum(cellChecksum);
                WritePrimaryKeyColumn(column, output, cellChecksum);
                rowChecksum = PlainBufferCrc8.crc8(rowChecksum, cellChecksum);
            }

            // 没有deleteMarker, 要与0x0做crc.
            rowChecksum = PlainBufferCrc8.crc8(rowChecksum, (byte)0x0);

            output.WriteRawByte(PlainBufferConsts.TAG_ROW_CHECKSUM);
            output.WriteRawByte(rowChecksum);

            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize primary key failed.");
            }

            return output.GetBuffer();
        }

        public static byte[] BuildRowPutChangeWithHeader(RowPutChange rowChange)
        {

            List<PlainBufferCell> pkCells = new List<PlainBufferCell>();

            if (rowChange.GetPrimaryKey() == null)
            {
                throw new ArgumentException("Primary Key is NULL");
            }

            foreach (PrimaryKeyColumn column in rowChange.GetPrimaryKey().GetPrimaryKeyColumns())
            {
                pkCells.Add(PlainBufferConversion.ToPlainBufferCell(column));
            }

            List<PlainBufferCell> cells = new List<PlainBufferCell>();
            foreach (Column column in rowChange.GetColumnsToPut())
            {
                cells.Add(PlainBufferConversion.ToPlainBufferCell(column, false, false, false, (byte)0x0));
            }

            PlainBufferRow row = new PlainBufferRow(pkCells, cells, false);

            int size = ComputePlainBufferRowWithHeader(row);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
            codedOutput.WriteRowWithHeader(row);
            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize row put change failed. Buffer remains " + output.Remain());
            }

            return output.GetBuffer();
        }

        public static byte[] BuildRowUpdateChangeWithHeader(RowUpdateChange rowChange)
        {
            List<PlainBufferCell> pkCells = new List<PlainBufferCell>();
            foreach (var column in rowChange.GetPrimaryKey().GetPrimaryKeyColumns())
            {
                pkCells.Add(PlainBufferConversion.ToPlainBufferCell(column));
            }

            List<PlainBufferCell> cells = new List<PlainBufferCell>();
            if (rowChange.GetColumnsToUpdate().Count > 0)
            {
                foreach (Tuple<Column, RowChangeType> column in rowChange.GetColumnsToUpdate())
                {
                    switch (column.Item2)
                    {
                        case RowChangeType.PUT:
                            cells.Add(PlainBufferConversion.ToPlainBufferCell(column.Item1, false, false, false, (byte)0x0));
                            break;
                        case RowChangeType.DELETE:
                            cells.Add(PlainBufferConversion.ToPlainBufferCell(column.Item1, true, false, true, PlainBufferConsts.DELETE_ONE_VERSION));
                            break;
                        case RowChangeType.DELETE_ALL:
                            cells.Add(PlainBufferConversion.ToPlainBufferCell(column.Item1, true, true, true, PlainBufferConsts.DELETE_ALL_VERSION));
                            break;
                        case RowChangeType.INCREMENT:
                            cells.Add(PlainBufferConversion.ToPlainBufferCell(column.Item1, false, true, true, PlainBufferConsts.INCREMENT));
                            break;
                    }
                }
            }

            PlainBufferRow row = new PlainBufferRow(pkCells, cells, false);

            int size = ComputePlainBufferRowWithHeader(row);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
            codedOutput.WriteRowWithHeader(row);

            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize row update change failed.");
            }

            return output.GetBuffer();
        }

        public static byte[] BuildRowDeleteChangeWithHeader(RowDeleteChange rowChange)
        {
            List<PlainBufferCell> pkCells = new List<PlainBufferCell>();
            foreach (var primaryKeyColumn in rowChange.GetPrimaryKey().GetPrimaryKeyColumns())
            {
                pkCells.Add(PlainBufferConversion.ToPlainBufferCell(primaryKeyColumn));
            }

            List<PlainBufferCell> cells = new List<PlainBufferCell>();
            PlainBufferRow row = new PlainBufferRow(pkCells, cells, true);

            int size = ComputePlainBufferRowWithHeader(row);
            PlainBufferOutputStream output = new PlainBufferOutputStream(size);
            PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
            codedOutput.WriteRowWithHeader(row);

            if (!output.IsFull())
            {
                throw new IOException("Bug: serialize row delete change failed.");
            }

            return output.GetBuffer();
        }
    }
}