using System;
using Aliyun.OTS.Util;
namespace Aliyun.OTS.DataModel
{
    /**
     * TableStore中每行数据都包含主键({@link PrimaryKey})，
     * 主键由多列主键列构成({@link PrimaryKeyColumn})，
     * 每一个主键列包含主键列名称和主键列的值{@link PrimaryKeyValue}。
     */
    public class Column: PrimaryKeyColumn
    {
        /// <summary>
        /// 序列化后占用的数据大小
        /// </summary>
        private int dataSize = -1;

        /// <summary>
        /// 属性列的时间戳
        /// </summary>
        public long? Timestamp { get; set; }

        public static NameTimestampComparator NAME_TIMESTAMP_COMPARATOR = new NameTimestampComparator();

        /// <summary>
        /// 根据指定的主键列的名称和主键列的值构造主键列。
        /// <p>主键列的名称不能为null pointer及空字符串。</p>
        /// <p>主键列的值不能为null pointer。</p>
        /// </summary>
        /// <param name="name">列名</param>
        /// <param name="columnValue">列值</param>
        public Column(string name, ColumnValue columnValue):base(name, columnValue)
        {
        }

        /// <summary>
        /// 构造一个属性列，必须包含名称、值和时间戳。
        /// <p>属性列的名称不能为null pointer及空字符串。</p>
        /// <p>属性列的值不能为null pointer。</p>
        /// </summary>
        /// <param name="name">列名</param>
        /// <param name="columnValue">列值</param>
        /// <param name="timestamp">列的时间戳</param>
        public Column(String name, ColumnValue columnValue, long timestamp):this(name, columnValue)
        {
            this.Timestamp = timestamp;
        }

        public new int GetDataSize()
        {
            if (dataSize == -1)
            {
                int size = OtsUtils.CalcStringSizeInBytes(Name) + Value.GetDataSize();
                if (this.Timestamp.HasValue)
                {
                    size += 8;
                }

                dataSize = size;
            }

            return dataSize;
        }

        public override string ToString()
        {
            return "'" + Name + "':" + Value + "," + Timestamp;
        }
    }
}
