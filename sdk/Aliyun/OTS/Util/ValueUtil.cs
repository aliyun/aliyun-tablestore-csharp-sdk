using Aliyun.OTS.DataModel;
using System;

namespace Aliyun.OTS.Util
{
    public class ValueUtil
    {
        public static ColumnValue ToColumnValue(object value)
        {
            if (value is ulong)
            {
                return new ColumnValue((ulong)value);
            }
            else if (value is Int64)
            {
                return new ColumnValue((Int64)value);
            }
            else if (value is double)
            {
                return new ColumnValue((double)value);
            }
            else if (value is bool)
            {
                return new ColumnValue((bool)value);
            }
            else if (value is byte[])
            {
                return new ColumnValue((byte[])value);
            }
            else
            {
                throw new ArgumentException(string.Format("unsupported type: {0}", value.GetType().Name));
            }
        }

        public static Object ToObject(ColumnValue value)
        {
            if (!value.Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            switch (value.Type)
            {
                case ColumnValueType.Integer:
                    return value.AsLong();
                case ColumnValueType.String:
                    return value.AsString();
                case ColumnValueType.Binary:
                    return value.AsBinary();
                case ColumnValueType.Boolean:
                    return value.AsBoolean();
                case ColumnValueType.Double:
                    return value.AsDouble();
                default:
                    throw new ArgumentException(string.Format("unsupported type: {0}", value.Type));
            }
        }
    }
}
