using System.IO;
using System.Collections.Generic;
using Google.ProtocolBuffers;
using Aliyun.OTS.DataModel;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferCodedInputStream
    {
        private readonly CodedInputStream inputStream;
        private uint lastTag = 0;

        public PlainBufferCodedInputStream(CodedInputStream inputStream)
        {
            this.inputStream = inputStream;
        }


        public List<PlainBufferRow> ReadRowsWithHeader()
        {
            List<PlainBufferRow> rows = new List<PlainBufferRow>();
            if (ReadHeader() != PlainBufferConsts.HEADER)
            {
                throw new IOException("Invalid header from plain buffer: " + this.inputStream);
            }

            ReadTag();
            while (!this.inputStream.IsAtEnd)
            {
                PlainBufferRow row = ReadRow();
                rows.Add(row);
            }

            if (!this.inputStream.IsAtEnd)
            {
                throw new IOException("");
            }

            return rows;
        }

        public uint ReadHeader()
        {
            return ReadUInt32();
        }

        public uint ReadTag()
        {
            if (this.inputStream.IsAtEnd)
            {
                lastTag = 0;
                return 0;
            }

            lastTag = this.inputStream.ReadRawByte();
            return lastTag;
        }

        public uint GetLastTag()
        {
            return this.lastTag;
        }


        public PlainBufferRow ReadRow()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_ROW_PK))
            {
                throw new IOException("Expect TAG_ROW_PK but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            List<PlainBufferCell> columns = new List<PlainBufferCell>();
            bool hasDeleteMarker = false;

            List<PlainBufferCell> primaryKey = ReadRowPK();
            if (primaryKey.Count <= 0)
            {
                throw new IOException("The primary key of row is empty.");
            }

            if (CheckLastTagWas(PlainBufferConsts.TAG_ROW_DATA))
            {
                columns = ReadRowData();
            }

            if (CheckLastTagWas(PlainBufferConsts.TAG_DELETE_ROW_MARKER))
            {
                hasDeleteMarker = true;
                ReadTag();
            }

            PlainBufferRow row = new PlainBufferRow(primaryKey, columns, hasDeleteMarker);

            row.SetExtension(ReadExtension());

            if (CheckLastTagWas(PlainBufferConsts.TAG_ROW_CHECKSUM))
            {
                byte checksum = this.inputStream.ReadRawByte();
                ReadTag();
                if (row.GetChecksum() != checksum)
                {
                    throw new IOException("Checksum is mismatch.Row: " + row + ". Checksum: " + checksum + ". PlainBuffer: " + this.inputStream);
                }
            }
            else
            {
                throw new IOException("Expect TAG_ROW_CHECKSUM but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            return row;
        }

        public bool CheckLastTagWas(uint tag)
        {
            return this.lastTag == tag;
        }

        public List<PlainBufferCell> ReadRowPK()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_ROW_PK))
            {
                throw new IOException("Expect TAG_ROW_PK but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            List<PlainBufferCell> primaryKeyColumns = new List<PlainBufferCell>();
            ReadTag();
            while (CheckLastTagWas(PlainBufferConsts.TAG_CELL))
            {
                PlainBufferCell cell = ReadCell();
                primaryKeyColumns.Add(cell);
            }

            return primaryKeyColumns;
        }

        public PlainBufferCell ReadCell()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_CELL))
            {
                throw new IOException("Expect TAG_CELL but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            PlainBufferCell cell = new PlainBufferCell();

            ReadTag();
            if (GetLastTag() == PlainBufferConsts.TAG_CELL_NAME)
            {
                cell.SetCellName(ReadUTFString(ReadUInt32()));
                ReadTag();
            }

            if (GetLastTag() == PlainBufferConsts.TAG_CELL_VALUE)
            {
                cell.SetCellValue(ReadCellValue());
            }

            if (GetLastTag() == PlainBufferConsts.TAG_CELL_TYPE)
            {
                cell.SetCellType(this.inputStream.ReadRawByte());
                ReadTag();
            }

            if (GetLastTag() == PlainBufferConsts.TAG_CELL_TIMESTAMP)
            {
                long timestamp = (long)ReadInt64();
                if (timestamp < 0)
                {
                    throw new IOException("The timestamp is negative.");
                }

                cell.SetCellTimestamp(timestamp);
                ReadTag(); // consume next tag as all read function should consume next tag
            }

            if (GetLastTag() == PlainBufferConsts.TAG_CELL_CHECKSUM)
            {
                byte checksum = this.inputStream.ReadRawByte();
                ReadTag();
                if (cell.GetChecksum() != checksum)
                {
                    throw new IOException("Checksum is mismatch. Cell: " + cell + ". Checksum: " + checksum + ". PlainBuffer: " + this.inputStream);
                }
            }
            else
            {
                throw new IOException("Expect TAG_CELL_CHECKSUM but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            return cell;
        }

        public List<PlainBufferCell> ReadRowData()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_ROW_DATA))
            {
                throw new IOException("Expect TAG_ROW_DATA but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            List<PlainBufferCell> columns = new List<PlainBufferCell>();
            ReadTag();
            while (CheckLastTagWas(PlainBufferConsts.TAG_CELL))
            {
                columns.Add(ReadCell());
            }

            return columns;
        }

        public PlainBufferExtension ReadExtension()
        {
            PlainBufferExtension extension = new PlainBufferExtension();
            if (CheckLastTagWas(PlainBufferConsts.TAG_EXTENSION))
            {
                ReadUInt32(); // length
                ReadTag();
                while (PlainBufferConsts.IsTagInExtension(GetLastTag()))
                {
                    if (CheckLastTagWas(PlainBufferConsts.TAG_SEQ_INFO))
                    {
                        extension.setSequenceInfo(ReadSequenceInfo());
                    }
                    else
                    {
                        int length = (int)this.inputStream.ReadRawLittleEndian32();
                        SkipRawSize(length);
                        ReadTag();
                    }
                }
            }

            return extension;
        }

        public PlainBufferSequenceInfo ReadSequenceInfo()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_SEQ_INFO))
            {
                throw new IOException("Expect TAG_SEQ_INFO but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            this.inputStream.ReadRawLittleEndian32();// length
            ReadTag();
            PlainBufferSequenceInfo seq = new PlainBufferSequenceInfo();

            if (CheckLastTagWas(PlainBufferConsts.TAG_SEQ_INFO_EPOCH))
            {
                seq.SetEpoch(ReadUInt32());
                ReadTag();
            }
            else
            {
                throw new IOException("Expect TAG_SEQ_INFO_EPOCH but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            if (CheckLastTagWas(PlainBufferConsts.TAG_SEQ_INFO_TS))
            {
                seq.SetTimestamp((long)ReadInt64());
                ReadTag();
            }
            else
            {
                throw new IOException("Expect TAG_SEQ_INFO_TS but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            if (CheckLastTagWas(PlainBufferConsts.TAG_SEQ_INFO_ROW_INDEX))
            {
                seq.SetRowIndex(ReadUInt32());
                ReadTag();
            }
            else
            {
                throw new IOException("Expect TAG_SEQ_INFO_ROW_INDEX but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }

            return seq;
        }

        public uint ReadUInt32()
        {
            return this.inputStream.ReadRawLittleEndian32();
        }

        public ulong ReadInt64()
        {
            return this.inputStream.ReadRawLittleEndian64();
        }

        public byte[] ReadBytes(uint size)
        {
            return this.inputStream.ReadRawBytes((int)size);
        }


        public string ReadUTFString(uint size)
        {
            return System.Text.Encoding.UTF8.GetString(ReadBytes(size));
        }

        public bool ReadBoolean()
        {
            bool result = false;
            this.inputStream.ReadBool(ref result);
            return result;
        }

        public double ReadDouble()
        {
            double result = 0;
            this.inputStream.ReadDouble(ref result);
            return result;
        }

        public void SkipRawSize(int length)
        {
            this.inputStream.SkipRawBytes(length);
        }


        public ColumnValue ReadCellValue()
        {
            if (!CheckLastTagWas(PlainBufferConsts.TAG_CELL_VALUE))
            {
                throw new IOException("Expect TAG_CELL_VALUE but it was " + PlainBufferConsts.PrintTag(GetLastTag()));
            }
            uint length = this.inputStream.ReadRawLittleEndian32();
            byte type = this.inputStream.ReadRawByte();
            ColumnValue columnValue = null;
            switch (type)
            {
                case PlainBufferConsts.VT_INTEGER:
                    columnValue = new ColumnValue(ReadInt64());
                    break;
                case PlainBufferConsts.VT_BLOB:
                    columnValue = new ColumnValue(ReadBytes(ReadUInt32()));
                    break;
                case PlainBufferConsts.VT_STRING:
                    columnValue = new ColumnValue(ReadUTFString(ReadUInt32()));
                    break;
                case PlainBufferConsts.VT_BOOLEAN:
                    columnValue = new ColumnValue(ReadBoolean());
                    break;
                case PlainBufferConsts.VT_DOUBLE:
                    columnValue = new ColumnValue(ReadDouble());
                    break;
                default:
                    throw new IOException("Unsupported column type: " + type);
            }

            ReadTag();
            return columnValue;
        }
    }
}
