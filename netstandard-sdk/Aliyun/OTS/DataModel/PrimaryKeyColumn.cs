using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using Aliyun.OTS.Util;
namespace Aliyun.OTS.DataModel
{
    public class PrimaryKeyColumn : IComparable, IMeasurable
    {
        /// <summary>
        /// 主键列名
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

        /// <summary>
        /// 列值
        /// </summary>
        public ColumnValue Value { get; set; }

        /// <summary>
        /// 序列化后占用的数据大小
        /// </summary>
        private int dataSize = -1;

        /// <summary>
        /// 根据指定的主键列的名称和主键列的值构造主键列。
        /// <p>主键列的名称不能为null pointer及空字符串。</p>
        /// <p>主键列的值不能为null pointer。</p>
        /// </summary>
        /// <param name="name">列名</param>
        /// <param name="columnValue">列值</param>
        public PrimaryKeyColumn(string name, ColumnValue columnValue)
        {
            Contract.Requires(!string.IsNullOrEmpty(name));
            Contract.Requires(columnValue != null);
            this.Name = name;
            this.Value = columnValue;
        }

        public PrimaryKeyColumn(KeyValuePair<string, ColumnValue> pair)
        {
            this.Name = pair.Key;
            this.Value = pair.Value;
        }

        public byte[] GetNameRawData()
        {
            return OtsUtils.String2Bytes(Name);
        }

        public int HashCode()
        {
            return Name.GetHashCode() ^ Value.GetHashCode();
        }

        public override int GetHashCode()
        {
            return HashCode();
        }

        public override bool Equals(Object obj)
        {
            if (obj == null || !(obj is PrimaryKeyColumn))
            {
                return false;
            }

            PrimaryKeyColumn col = (PrimaryKeyColumn)obj;
            return this.Name.Equals(col.Name) && this.Value.Equals(col.Value);
        }

        /// <summary>
        /// 比较两个主键列的大小
        /// <p>对比的两个主键列必须含有相同的名称和类型。</p>
        /// </summary>
        /// <returns>若相等返回0，若大于返回1，若小于返回-1</returns>
        /// <param name="obj">比较对象</param>
        public int CompareTo(Object obj)
        {
            var target = obj as PrimaryKeyColumn;
            if (!this.Name.Equals(target.Name))
            {
                throw new ArgumentException("The name of primary key to be compared must be the same.");
            }

            return this.Value.CompareTo(target.Value);
        }

        public int GetDataSize()
        {
            if (dataSize == -1)
            {
                dataSize = OtsUtils.CalcStringSizeInBytes(Name) + Value.GetDataSize();
            }

            return dataSize;
        }

        public override string ToString()
        {
            return "'" + Name + "':" + Value;
        }

        public Column ToColumn()
        {
            return new Column(this.Name, this.Value);
        }
    }
}
