using System;
using System.IO;
using System.Text;
using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchVariantType
    {
        public enum VariantType
        {
            INTEGER,
            DOUBLE,
            BOOLEAN,
            STRING
        }

        // variant type
        public static byte VT_INTEGER = 0x0;
        public static byte VT_DOUBLE = 0x1;
        public static byte VT_BOOLEAN = 0x2;
        public static byte VT_STRING = 0x3;

        public static VariantType GetVariantType(byte[] data)
        {
            if (data[0] == VT_INTEGER)
            {
                return VariantType.INTEGER;
            }
            else if (data[0] == VT_DOUBLE)
            {
                return VariantType.DOUBLE;
            }
            else if (data[0] == VT_BOOLEAN)
            {
                return VariantType.BOOLEAN;
            }
            else if (data[0] == VT_STRING)
            {
                return VariantType.STRING;
            }
            else
            {
                throw new ArgumentException("unknown type: " + data[0]);
            }
        }

        public static byte[] FromLong(long v)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.WriteByte(VT_INTEGER);
                foreach (var item in BitConverter.GetBytes(v))
                {
                    ms.WriteByte(item);
                }
                return ms.ToArray();
            }
        }

        public static byte[] FromDouble(double v)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.WriteByte(VT_DOUBLE);
                foreach (var item in BitConverter.GetBytes(v))
                {
                    ms.WriteByte(item);
                }
                return ms.ToArray();
            }
        }

        public static byte[] FromString(string v)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                byte[] strBytes = UTF8Encoding.Default.GetBytes(v);

                ms.WriteByte(VT_STRING);
                foreach (var item in BitConverter.GetBytes(strBytes.Length))
                {
                    ms.WriteByte(item);
                }

                foreach (var item in UTF8Encoding.Default.GetBytes(v))
                {
                    ms.WriteByte(item);
                }
                return ms.ToArray();
            }
        }

        public static byte[] FromBoolean(bool v)
        {
            return new byte[] { VT_BOOLEAN, (byte)(v ? 1 : 0) };
        }

        public static byte[] toVariant(ColumnValue value)
        {
            switch (value.Type)
            {
                case ColumnValueType.String:
                    return FromString(value.StringValue);
                case ColumnValueType.Integer:
                    return FromLong(value.IntegerValue);
                case ColumnValueType.Double:
                    return FromDouble(value.DoubleValue);
                case ColumnValueType.Boolean:
                    return FromBoolean(value.BooleanValue);
                default:
                    throw new ArgumentException("unsupported type:" + value.Type);
            }
        }
    }
}
