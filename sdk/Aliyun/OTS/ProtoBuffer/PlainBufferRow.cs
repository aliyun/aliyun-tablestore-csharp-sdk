using System;
using System.Collections.Generic;
using System.Text;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferRow
    {
        private List<PlainBufferCell> primaryKey;

        private readonly List<PlainBufferCell> cells;

        private bool hasDeleteMarker = false;

        private byte checksum;
        private bool hasChecksum = false;
        private PlainBufferExtension extension;

        public PlainBufferRow(List<PlainBufferCell> primaryKey, List<PlainBufferCell> cells, bool hasDeleteMarker)
        {
            this.primaryKey = primaryKey;
            this.cells = cells;
            this.hasDeleteMarker = hasDeleteMarker;
            this.extension = new PlainBufferExtension();
        }

        public List<PlainBufferCell> GetPrimaryKey()
        {
            return primaryKey;
        }

        public void SetPrimaryKey(List<PlainBufferCell> primaryKey)
        {
            this.primaryKey = primaryKey;
            this.hasChecksum = false;
        }

        public List<PlainBufferCell> GetCells()
        {
            return cells;
        }

        public void AddCell(PlainBufferCell cell)
        {
            this.cells.Add(cell);
            this.hasChecksum = false;
        }

        public bool HasCells()
        {
            return this.cells.Count > 0;
        }

        public bool HasDeleteMarker()
        {
            return hasDeleteMarker;
        }

        public void SetHasDeleteMarker(bool hasDeleteMarker)
        {
            this.hasDeleteMarker = hasDeleteMarker;
            this.hasChecksum = false;
        }

        public PlainBufferExtension GetExtension()
        {
            return extension;
        }

        public void SetExtension(PlainBufferExtension extension)
        {
            this.extension = extension;
        }

        public bool HasExtension()
        {
            return extension.HasSeq();
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


        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("PrimaryKey: ").Append(primaryKey);
            sb.Append("Cells: ");
            foreach (PlainBufferCell cell in cells)
            {
                sb.Append("[").Append(cell).Append("]");
            }

            sb.Append(" HasDeleteMarker: " + HasDeleteMarker());

            if (HasExtension())
            {
                sb.Append(" Extension: {");
                sb.Append(GetExtension());
                sb.Append("}");
            }
            return sb.ToString();
        }
    }
}
