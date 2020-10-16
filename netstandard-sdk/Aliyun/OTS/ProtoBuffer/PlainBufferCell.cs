using System;
using Aliyun.OTS.DataModel;
using System.IO;
using Aliyun.OTS;
using System.Text;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferCell
    {

        private String cellName;
        private byte[] nameRawData;
        private bool hasCellName;

        private ColumnValue cellValue;
        private bool hasCellValue;

        private bool isPk;
        private ColumnValue pkCellValue;

        private byte cellType;
        private bool hasCellType;

        private long cellTimestamp;
        private bool hasCellTimestamp;

        private byte checksum;
        private bool hasChecksum;

        public String GetCellName()
        {
            return cellName;
        }

        public byte[] GetNameRawData()
        {
            if (nameRawData == null)
            {
                this.nameRawData = Encoding.UTF8.GetBytes(cellName);
            }

            return nameRawData;
        }

        public void SetCellName(String cellName)
        {
            this.cellName = cellName;
            this.hasCellName = true;
            this.nameRawData = null;
            this.hasChecksum = false;
        }

        public bool HasCellName()
        {
            return this.hasCellName;
        }

        public ColumnValue GetCellValue()
        {
            return this.cellValue;
        }

        public void SetCellValue(ColumnValue cellValue)
        {
            this.cellValue = cellValue;
            this.hasCellValue = true;
            this.hasChecksum = false;
        }

        public bool HasCellValue()
        {
            return this.hasCellValue;
        }

        public byte GetCellType()
        {
            return this.cellType;
        }

        public void SetCellType(byte cellType)
        {
            this.cellType = cellType;
            this.hasCellType = true;
            this.hasChecksum = false;
        }

        public bool HasCellType()
        {
            return this.hasCellType;
        }

        public long GetCellTimestamp()
        {
            return this.cellTimestamp;
        }

        public void SetCellTimestamp(long cellTimestamp)
        {
            this.cellTimestamp = cellTimestamp;
            this.hasCellTimestamp = true;
            this.hasChecksum = false;
        }

        public bool HasCellTimestamp()
        {
            return hasCellTimestamp;
        }

        /// <summary>
        /// 会自动计算当前的checksum并返回，当没有数据变化时，checksum会缓存在对象中，以减少不必要的计算。
        /// </summary>
        /// <returns>The checksum.</returns>
        public byte GetChecksum()
        {
            if (!this.hasChecksum)
            {
                GenerateChecksum();
            }

            return this.checksum;
        }

        private void GenerateChecksum()
        {
            this.checksum = PlainBufferCrc8.GetChecksum((byte)0x0, this);
            this.hasChecksum = true;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(Object obj)
        {
            if (obj == null || !(obj is PlainBufferCell))
            {
                return false;
            }

            try
            {
                if (GetChecksum() != ((PlainBufferCell)obj).GetChecksum())
                {
                    return false;
                }
            }
            catch (IOException e)
            {
                throw new OTSException("Error when getChecksum." + e.Message);
            }

            if ((HasCellName() != ((PlainBufferCell)obj).HasCellName())
                    || (HasCellName() && !GetCellName().Equals(((PlainBufferCell)obj).GetCellName())))
            {
                return false;
            }

            if ((HasCellValue() != ((PlainBufferCell)obj).HasCellValue())
                    || (HasCellValue() && !GetCellValue().Equals(((PlainBufferCell)obj).GetCellValue())))
            {
                return false;
            }

            if ((IsPk() != ((PlainBufferCell)obj).IsPk())
                    || (IsPk() && !GetPkCellValue().Equals(((PlainBufferCell)obj).GetPkCellValue())))
            {
                return false;
            }

            if ((HasCellType() != ((PlainBufferCell)obj).HasCellType())
                    || (HasCellType() && (GetCellType() != ((PlainBufferCell)obj).GetCellType())))
            {
                return false;
            }

            if ((HasCellTimestamp() != ((PlainBufferCell)obj).HasCellTimestamp())
                    || (HasCellTimestamp() && (GetCellTimestamp() != ((PlainBufferCell)obj).GetCellTimestamp())))
            {
                return false;
            }

            return true;
        }


        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("CellName: " + HasCellName() + "|" + cellName);
            sb.Append(", CellValue: " + HasCellValue() + "|" + cellValue);
            sb.Append(", CellType: " + HasCellType() + "|" + cellType);
            sb.Append(", IsPk: " + IsPk() + "|" + GetPkCellValue());
            sb.Append(", CellTimestamp: " + HasCellTimestamp() + "|" + cellTimestamp);
            sb.Append(", Checksum: " + this.hasChecksum + "|" + checksum);
            return sb.ToString();
        }

        public bool IsPk()
        {
            return isPk;
        }

        public ColumnValue GetPkCellValue()
        {
            return pkCellValue;
        }

        public void SetPkCellValue(ColumnValue pkCellValue)
        {
            this.pkCellValue = pkCellValue;
            this.hasCellValue = true;
            this.hasChecksum = false;
            this.isPk = true;
        }
    }

}
