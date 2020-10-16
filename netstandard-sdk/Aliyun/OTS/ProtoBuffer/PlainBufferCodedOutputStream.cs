using System.IO;
using Aliyun.OTS.DataModel;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferCodedOutputStream
    {
        private readonly PlainBufferOutputStream output;

        public PlainBufferCodedOutputStream(PlainBufferOutputStream output)
        {
            this.output = output;
        }

        public void WriteHeader()
        {
            output.WriteRawLittleEndian32(PlainBufferConsts.HEADER);
        }

        public void WriteTag(byte tag)
        {
            output.WriteRawByte(tag);
        }

        public void WriteCellName(byte[] name)
        {
            WriteTag(PlainBufferConsts.TAG_CELL_NAME);
            output.WriteRawLittleEndian32(name.Length);
            output.WriteBytes(name);
        }

        public void WritePrimaryKeyValue(ColumnValue value)
        {
            if (value.CanBePrimaryKeyValue())
            {
                WriteCellValue(value);
            }
            else
            {
                throw new IOException("Bug: unsupported primary key type: " + value.GetType());
            }
        }

        public void WriteCellValue(ColumnValue value)
        {
            WriteTag(PlainBufferConsts.TAG_CELL_VALUE);
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

            byte[] rawData;
            int prefixLength;

            switch (value.Type)
            {
                case ColumnValueType.String:
                    rawData = value.AsStringInBytes();
                    prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                    output.WriteRawLittleEndian32(prefixLength + rawData.Length); // length + type + value
                    output.WriteRawByte(PlainBufferConsts.VT_STRING);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;
                case ColumnValueType.Integer:
                    output.WriteRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                    output.WriteRawByte(PlainBufferConsts.VT_INTEGER);
                    output.WriteRawLittleEndian64(value.IntegerValue);
                    break;
                case ColumnValueType.Binary:
                    rawData = value.BinaryValue;
                    prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                    output.WriteRawLittleEndian32(prefixLength + rawData.Length); // length + type + value
                    output.WriteRawByte(PlainBufferConsts.VT_BLOB);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;
                case ColumnValueType.Boolean:
                    output.WriteRawLittleEndian32(2);
                    output.WriteRawByte(PlainBufferConsts.VT_BOOLEAN);
                    output.WriteBool(value.BooleanValue);
                    break;
                case ColumnValueType.Double:
                    output.WriteRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                    output.WriteRawByte(PlainBufferConsts.VT_DOUBLE);
                    output.WriteDouble(value.DoubleValue);
                    break;
                default:
                    throw new IOException("Bug: unsupported column type: " + value.Type);
            }
        }

        public void WriteCell(PlainBufferCell cell)
        {
            WriteTag(PlainBufferConsts.TAG_CELL);

            if (cell.HasCellName())
            {
                WriteCellName(cell.GetNameRawData());
            }

            if (cell.HasCellValue())
            {
                var columnValue = cell.IsPk() ? cell.GetPkCellValue() : cell.GetCellValue();
                WriteCellValue(columnValue);
            }

            if (cell.HasCellType())
            {
                WriteTag(PlainBufferConsts.TAG_CELL_TYPE);
                output.WriteRawByte(cell.GetCellType());
            }

            if (cell.HasCellTimestamp())
            {
                WriteTag(PlainBufferConsts.TAG_CELL_TIMESTAMP);
                output.WriteRawLittleEndian64(cell.GetCellTimestamp());
            }

            WriteTag(PlainBufferConsts.TAG_CELL_CHECKSUM);
            output.WriteRawByte(cell.GetChecksum());
        }

        public void WriteExtension(PlainBufferExtension extension)
        {
            WriteTag(PlainBufferConsts.TAG_EXTENSION);
            output.WriteRawLittleEndian32(PlainBufferBuilder.ComputeSkipLengthForExtensionTag(extension));
            int extensionCount = 0;
            if (extension.HasSeq())
            {
                WriteSequenceInfo(extension.GetSequenceInfo());
                extensionCount++;
            }

            if (extensionCount == 0)
            {
                throw new IOException("no extension tag is Writen.");
            }
        }

        public void WriteSequenceInfo(PlainBufferSequenceInfo sequenceInfo)
        {
            WriteTag(PlainBufferConsts.TAG_SEQ_INFO);
            output.WriteRawLittleEndian32(PlainBufferBuilder.ComputeSkipLengthForSequenceInfo());
            WriteTag(PlainBufferConsts.TAG_SEQ_INFO_EPOCH);
            output.WriteRawLittleEndian32((int)sequenceInfo.GetEpoch());
            WriteTag(PlainBufferConsts.TAG_SEQ_INFO_TS);
            output.WriteRawLittleEndian64(sequenceInfo.GetTimestamp());
            WriteTag(PlainBufferConsts.TAG_SEQ_INFO_ROW_INDEX);
            output.WriteRawLittleEndian32((int)sequenceInfo.GetRowIndex());
        }

        public void WriteRow(PlainBufferRow row)
        {
            WriteTag(PlainBufferConsts.TAG_ROW_PK);
            foreach (PlainBufferCell cell in row.GetPrimaryKey())
            {
                WriteCell(cell);
            }

            if (row.GetCells().Count > 0)
            {
                WriteTag(PlainBufferConsts.TAG_ROW_DATA);
                foreach (PlainBufferCell cell in row.GetCells())
                {
                    WriteCell(cell);
                }
            }
            if (row.HasDeleteMarker())
            {
                WriteTag(PlainBufferConsts.TAG_DELETE_ROW_MARKER);
            }

            if (row.HasExtension())
            {
                WriteExtension(row.GetExtension());
            }

            WriteTag(PlainBufferConsts.TAG_ROW_CHECKSUM);
            output.WriteRawByte(row.GetChecksum());
        }

        public void WriteRowWithHeader(PlainBufferRow row)
        {
            WriteHeader();
            WriteRow(row);
        }

        public void WritePrimaryKeyValueWithoutLengthPrefix(ColumnValue value)
        {
            if (value.IsInfMin())
            {
                output.WriteRawByte(PlainBufferConsts.VT_INF_MIN);
                return;
            }

            if (value.IsInfMax())
            {
                output.WriteRawByte(PlainBufferConsts.VT_INF_MAX);
                return;
            }

            if (value.IsPlaceHolderForAutoIncr())
            {
                output.WriteRawByte(PlainBufferConsts.VT_AUTO_INCREMENT);
                return;
            }

            byte[] rawData;

            switch (value.Type)
            {
                case ColumnValueType.String:
                    rawData = value.AsStringInBytes();
                    output.WriteRawByte(PlainBufferConsts.VT_STRING);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;
                case ColumnValueType.Integer:
                    output.WriteRawByte(PlainBufferConsts.VT_INTEGER);
                    output.WriteRawLittleEndian64(value.IntegerValue);
                    break;
                case ColumnValueType.Binary:
                    rawData = value.BinaryValue;
                    output.WriteRawByte(PlainBufferConsts.VT_BLOB);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;

                default:
                    throw new IOException("Bug: unsupported primary key type: " + value.Type);
            }
        }


        public void WriteColumnValueWithoutLengthPrefix(ColumnValue value)
        {
            byte[] rawData;
            switch (value.Type)
            {
                case ColumnValueType.String:
                    rawData = value.AsStringInBytes();
                    output.WriteRawByte(PlainBufferConsts.VT_STRING);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;
                case ColumnValueType.Integer:
                    output.WriteRawByte(PlainBufferConsts.VT_INTEGER);
                    output.WriteRawLittleEndian64(value.IntegerValue);
                    break;
                case ColumnValueType.Binary:
                    rawData = value.BinaryValue;
                    output.WriteRawByte(PlainBufferConsts.VT_BLOB);
                    output.WriteRawLittleEndian32(rawData.Length);
                    output.WriteBytes(rawData);
                    break;

                case ColumnValueType.Double:
                    output.WriteRawByte(PlainBufferConsts.VT_DOUBLE);
                    output.WriteDouble(value.DoubleValue);
                    break;
                case ColumnValueType.Boolean:
                    output.WriteRawByte(PlainBufferConsts.VT_BOOLEAN);
                    output.WriteBool(value.BooleanValue);
                    break;
                default:
                    throw new IOException("Bug: unsupported column type: " + value.Type);
            }
        }
    }
}