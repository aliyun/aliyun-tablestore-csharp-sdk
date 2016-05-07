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

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 列值的类型枚举定义
    /// </summary>
    public enum ColumnValueType 
    { 
        /// <summary>
        /// 字符串类型
        /// </summary>
        String,
        
        /// <summary>
        /// 整数类型
        /// </summary>
        Integer,
        
        /// <summary>
        /// 布尔类型
        /// </summary>
        Boolean,
        
        /// <summary>
        /// 浮点数类型
        /// </summary>
        Double,
        
        /// <summary>
        /// 二进制串类型
        /// </summary>
        Binary,
    }

    /// <summary>
    /// 表示一个列值，包含类型和值。
    /// </summary>
    public class ColumnValue
    {
        /// <summary>
        /// 表示一个最小值（不分类型）
        /// </summary>
        public static ColumnValue INF_MAX = new ColumnValue();
        
        /// <summary>
        /// 表示一个最大值（不分类型）
        /// </summary>
        public static ColumnValue INF_MIN = new ColumnValue();
        
        /// <summary>
        /// 列值的类型
        /// </summary>
        public ColumnValueType Type { get; private set;}
        
        public Int64 IntegerValue = 0;
        public string StringValue = null;
        public bool BooleanValue = true;
        public double DoubleValue = 0;
        public byte[] BinaryValue = null;
        
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
    }
}
