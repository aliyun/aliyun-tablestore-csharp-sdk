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
                throw new ArgumentException(string.Format("unknown type: {0}", data[0]));
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

        public static long AsLong(byte[] data)
        {
            if (data.Length - 1 < sizeof(long))
            {
                throw new InvalidOperationException(string.Format("data.length[{0}] < sizeof(long)", data.Length - 1));
            }
            // 小端序 低端 --> 高端
            return BitConverter.ToInt64(data, 1);
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

        public static double AsDouble(byte[] data)
        {
            if (data.Length - 1 < sizeof(double))
            {
                throw new InvalidOperationException(string.Format("data.length[{0}] < sizeof(double)", data.Length - 1));
            }

            return BitConverter.ToDouble(data, 1);
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

        public static string AsString(byte[] data)
        {
            if (data.Length - 1 < 0)
            {
                throw new InvalidOperationException(string.Format("data.length[{0}] < 0", data.Length - 1));
            }

            int length = BitConverter.ToInt32(data, 1);
            byte[] ValueData = new byte[length];
            Array.Copy(data, 5, ValueData, 0, length);

            return Encoding.UTF8.GetString(ValueData);
        }

        public static byte[] FromBoolean(bool v)
        {
            return new byte[] { VT_BOOLEAN, (byte)(v ? 1 : 0) };
        }

        public static bool AsBoolean(byte[] data)
        {
            if (data.Length - 1 < sizeof(bool))
            {
                throw new InvalidOperationException(string.Format("data.length[{0}] < sizeof(bool)", data.Length - 1));
            }
            return BitConverter.ToBoolean(data, 1);
        }

        public static byte[] ToVariant(ColumnValue value)
        {
            if (!value.Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

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
                    throw new ArgumentException(string.Format("unsupported type: {0}" , value.Type));
            }
        }

        public static Object GetValue(byte[] data)
        {
            if (data[0] == VT_INTEGER)
            {
                return AsLong(data);
            }
            else if (data[0] == VT_DOUBLE)
            {
                return AsDouble(data);
            }
            else if (data[0] == VT_STRING)
            {
                return AsString(data);
            }
            else if (data[0] == VT_BOOLEAN)
            {
                return AsBoolean(data);
            }
            else
            {
                throw new ArgumentException(string.Format("unsupported type: {0}", data[0]));
            }
        }

        public static ColumnValue ForceConvertToDestColumnValue(byte[] data)
        {
            if (data.Length == 0)
            {
                throw new IOException("data is null");
            }

            ColumnValue columnValue = null;

            if (data[0] == VT_INTEGER)
            {
                columnValue = new ColumnValue(AsLong(data));
            }
            else if (data[0] == VT_STRING)
            {
                columnValue = new ColumnValue(AsString(data));
            }
            else if (data[0] == VT_DOUBLE)
            {
                columnValue = new ColumnValue(AsDouble(data));
            }
            else if (data[0] == VT_BOOLEAN)
            {
                columnValue = new ColumnValue(AsBoolean(data));
            }
            else
            {
                throw new IOException(string.Format("unsupport type: {0}", data[0]));
            }

            return columnValue;
        }
    }
}
