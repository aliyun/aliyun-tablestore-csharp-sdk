using System;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public static class PlainBufferCrc8
    {
        private const int spaceSize = 256;
        private static byte[] crc8Table = new byte[spaceSize];

        static PlainBufferCrc8()
        {
            for (int i = 0; i < crc8Table.Length; ++i)
            {
                byte x = (byte)i;
                for (int j = 8; j > 0; --j)
                {
                    x = (byte)((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
                }
                crc8Table[i] = x;
            }
        }

        public static byte crc8(byte crc, byte data)
        {
            crc = crc8Table[(crc ^ data) & 0xff];
            return crc;
        }

        public static byte crc8(byte crc, int data)
        {
            for (int i = 0; i < 4; i++)
            {
                crc = crc8(crc, (byte)(data & 0xff));
                data >>= 8;
            }

            return crc;
        }

        public static byte crc8(byte crc, long data)
        {
            for (int i = 0; i < 8; i++)
            {
                crc = crc8(crc, (byte)(data & 0xff));
                data >>= 8;
            }
            return crc;
        }

        public static byte crc8(byte crc, byte[] data)
        {
            for (int i = 0; i < data.Length; i++)
            {
                crc = crc8(crc, data[i]);
            }
            return crc;
        }

        public static byte GetChecksum(byte crc, PlainBufferCell cell)
        {

            if (cell.HasCellName())
            {
                crc = crc8(crc, cell.GetNameRawData());
            }

            if (cell.HasCellValue())
            {
                if (cell.IsPk())
                {
                    crc = cell.GetPkCellValue().GetChecksum(crc);
                }
                else
                {
                    crc = cell.GetCellValue().GetChecksum(crc);
                }
            }

            if (cell.HasCellTimestamp())
            {
                crc = crc8(crc, cell.GetCellTimestamp());
            }

            if (cell.HasCellType())
            {
                crc = crc8(crc, cell.GetCellType());
            }

            return crc;
        }

        public static byte GetChecksum(byte crc, PlainBufferRow row)
        {
            foreach (PlainBufferCell cell in row.GetPrimaryKey())
            {
                crc = crc8(crc, cell.GetChecksum());
            }

            foreach (PlainBufferCell cell in row.GetCells())
            {
                crc = crc8(crc, cell.GetChecksum());
            }

            byte del = 0;
            if (row.HasDeleteMarker())
            {
                del = (byte)0x1;
            }
            crc = crc8(crc, del);

            return crc;
        }
    }
}
