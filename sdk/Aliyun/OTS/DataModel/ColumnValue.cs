/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System;
using System.IO;
using Aliyun.OTS.Util;
using com.alicloud.openservices.tablestore.core.protocol;


namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表示一个列值，包含类型和值。
    /// </summary>
    public class ColumnValue : IComparable, IMeasurable
    {
        /// <summary>
        /// 表示一个最大值（不分类型）
        /// </summary>
        public readonly static ColumnValue INF_MAX = new ColumnValue("INF_MAX");

        /// <summary>
        /// 表示一个最小值（不分类型）
        /// </summary>
        public readonly static ColumnValue INF_MIN = new ColumnValue("INF_MIN");

        /// <summary>
        /// 表示一个自增值（不分类型）
        /// </summary>
        public readonly static ColumnValue AUTO_INCREMENT = new ColumnValue("AUTO_INCREMENT");

        /// <summary>
        /// 列值的类型
        /// </summary>
        public ColumnValueType? Type { get; private set; }

        public Int64 IntegerValue;
        public string StringValue;
        public bool BooleanValue = true;
        public double DoubleValue;
        public byte[] BinaryValue;

        private ColumnValue()
        {
        }

        /// <summary>
        /// 构造一个整数类型的列值。
        /// </summary>
        /// <param name="value"></param>
        public ColumnValue(Int64 value)
        {
            Type = ColumnValueType.Integer;
            IntegerValue = value;
        }

        public ColumnValue(ulong value)
        {
            Type = ColumnValueType.Integer;
            IntegerValue = (Int64)value;
        }

        /// <summary>
        /// 构造一个字符串类型的列值。
        /// </summary>
        /// <param name="value"></param>
        public ColumnValue(string value)
        {
            Type = ColumnValueType.String;
            StringValue = value;
        }

        /// <summary>
        /// 构造一个布尔类型的列值。
        /// </summary>
        /// <param name="value"></param>
        public ColumnValue(bool value)
        {
            Type = ColumnValueType.Boolean;
            BooleanValue = value;
        }

        /// <summary>
        /// 构造一个浮点类型的列值。
        /// </summary>
        /// <param name="value"></param>
        public ColumnValue(double value)
        {
            Type = ColumnValueType.Double;
            DoubleValue = value;
        }

        /// <summary>
        /// 构造一个二进制串类型的列值。
        /// </summary>
        /// <param name="value"></param>
        public ColumnValue(byte[] value)
        {
            Type = ColumnValueType.Binary;
            BinaryValue = value;
        }

        /// <summary>
        /// 这个column value是否可以为 primary key value
        /// </summary>
        /// <returns><c>true</c>, column value类型可以成为primary key, <c>false</c> otherwise.</returns>
        public bool CanBePrimaryKeyValue()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            switch (Type)
            {
                case ColumnValueType.String:
                case ColumnValueType.Integer:
                case ColumnValueType.Binary:
                    return true;
                default:
                    return false;
            }
        }

        public byte[] AsStringInBytes()
        {
            return System.Text.Encoding.UTF8.GetBytes(StringValue);
        }

        public bool IsInfMin()
        {
            return StringValue == "INF_MIN";
        }

        public bool IsInfMax()
        {
            return StringValue == "INF_MAX";
        }

        public bool IsPlaceHolderForAutoIncr()
        {
            return StringValue == "AUTO_INCREMENT";
        }

        public long AsLong()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (Type != ColumnValueType.Integer)
            {
                throw new ArgumentException(string.Format("The type of column is not INTEGER but {0}", Type.ToString()));
            }

            return IntegerValue;
        }

        public string AsString()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (Type != ColumnValueType.String)
            {
                throw new ArgumentException(string.Format("The type of column is not STRING but {0}", Type.ToString()));
            }

            return StringValue;
        }

        public byte[] AsBinary()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (Type != ColumnValueType.Binary)
            {
                throw new ArgumentException(string.Format("The type of column is not BINARY but {0}", Type.ToString()));
            }

            return BinaryValue;
        }

        public bool AsBoolean()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (Type != ColumnValueType.Boolean)
            {
                throw new ArgumentException(string.Format("The type of column is not BOOLEAN but {0}", Type.ToString()));
            }

            return BooleanValue;
        }

        public double AsDouble()
        {
            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (Type != ColumnValueType.Double)
            {
                throw new ArgumentException(string.Format("The type of column is not DOUBLE but {0}", Type.ToString()));
            }

            return DoubleValue;
        }

        /// <summary>
        ///  采用crc8算法得到一个checksum，主要用于计算cell的checksum
        /// </summary>
        /// <returns>The checksum.</returns>
        /// <param name="crc">Crc.</param>
        public byte GetChecksum(byte crc)
        {
            if (IsInfMin())
            {
                crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_INF_MIN);
                return crc;
            }

            if (IsInfMax())
            {
                crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_INF_MAX);
                return crc;
            }

            if (IsPlaceHolderForAutoIncr())
            {
                crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_AUTO_INCREMENT);
                return crc;
            }

            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            switch (Type)
            {
                case ColumnValueType.String:
                    {
                        byte[] rawData = AsStringInBytes();
                        crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_STRING);
                        crc = PlainBufferCrc8.crc8(crc, rawData.Length);
                        crc = PlainBufferCrc8.crc8(crc, rawData);
                        break;
                    }
                case ColumnValueType.Integer:
                    {
                        crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_INTEGER);
                        crc = PlainBufferCrc8.crc8(crc, (long)this.IntegerValue);
                        break;
                    }
                case ColumnValueType.Binary:
                    {
                        byte[] rawData = this.BinaryValue;
                        crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_BLOB);
                        crc = PlainBufferCrc8.crc8(crc, rawData.Length);
                        crc = PlainBufferCrc8.crc8(crc, rawData);
                        break;
                    }
                case ColumnValueType.Double:
                    {
                        crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_DOUBLE);
                        crc = PlainBufferCrc8.crc8(crc, BitConverter.DoubleToInt64Bits(this.DoubleValue));
                        break;
                    }
                case ColumnValueType.Boolean:
                    {
                        crc = PlainBufferCrc8.crc8(crc, PlainBufferConsts.VT_BOOLEAN);
                        crc = PlainBufferCrc8.crc8(crc, this.BooleanValue ? (byte)0x1 : (byte)0x0);
                        break;
                    }
                default:
                    throw new IOException("Bug: unsupported column type: " + this.Type);
            }

            return crc;
        }

        public int GetDataSize()
        {
            int dataSize = 0;

            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            switch (this.Type)
            {
                case ColumnValueType.Integer:
                    dataSize = 8;
                    break;
                case ColumnValueType.String:

                    dataSize = this.StringValue == null ? 0 : OtsUtils.CalcStringSizeInBytes(this.StringValue);
                    break;
                case ColumnValueType.Binary:
                    dataSize = this.BinaryValue.Length;
                    break;
                case ColumnValueType.Double:
                    dataSize = 8;
                    break;
                case ColumnValueType.Boolean:
                    dataSize = 1;
                    break;
                default:
                    throw new TypeLoadException("Bug: not support the type : " + this.Type);
            }

            return dataSize;
        }

        public int CompareTo(Object obj)
        {
            var target = obj as ColumnValue;

            if (!this.Type.HasValue || !target.Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            if (this.Type != target.Type)
            {
                throw new ArgumentException("The type of column to compare must be the same.");
            }

            switch (this.Type)
            {
                case ColumnValueType.String:
                    return string.Compare(this.StringValue, target.StringValue, StringComparison.Ordinal);
                case ColumnValueType.Integer:
                    return this.IntegerValue.CompareTo(target.IntegerValue);
                case ColumnValueType.Binary:
                    int ret = OtsUtils.CompareByteArrayInLexOrder(this.BinaryValue, 0, this.BinaryValue.Length, target.BinaryValue, 0, target.BinaryValue.Length);
                    return ret;
                case ColumnValueType.Double:
                    return this.DoubleValue.CompareTo(target.DoubleValue);
                case ColumnValueType.Boolean:
                    return this.BooleanValue.CompareTo(target.BooleanValue);
                default:
                    throw new ArgumentException("Unknown type: " + this.Type);
            }
        }

        public override string ToString()
        {
            string result = "";

            if (!Type.HasValue)
            {
                throw new ArgumentNullException("The type of the column is not set");
            }

            switch (Type)
            {
                case ColumnValueType.String:
                    result = StringValue + "(string)";
                    break;
                case ColumnValueType.Integer:
                    result = IntegerValue + "(int)";
                    break;
                case ColumnValueType.Binary:
                    result = BinaryValue + "(binary)";
                    break;
                case ColumnValueType.Double:
                    result = DoubleValue + "(double)";
                    break;
                case ColumnValueType.Boolean:
                    result = BooleanValue + "(boolean)";
                    break;
                default:
                    throw new ArgumentException("Unknown type: " + this.Type);
            }

            return result;
        }
    }
}
