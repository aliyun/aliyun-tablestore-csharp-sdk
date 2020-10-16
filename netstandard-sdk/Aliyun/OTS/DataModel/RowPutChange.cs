using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{
    public class RowPutChange : RowChange
    {
        /// <summary>
        /// 行的属性列集合。
        /// </summary>
        private readonly List<Column> columnsToPut = new List<Column>();

        private long? timestamp;

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="tableName">表的名称</param>
        public RowPutChange(String tableName) : base(tableName)
        {
        }

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="tableName">表的名称</param>
        /// <param name="primaryKey">行的主键</param>
        public RowPutChange(String tableName, PrimaryKey primaryKey) : base(tableName, primaryKey)
        {
        }

        /// <summary>
        /// 构造函数。
        /// <p>允许用户设置一个默认的时间戳，若写入的列没有带时间戳，则会使用该默认时间戳。</p>
        /// </summary>
        /// <param name="tableName">表的名称.</param>
        /// <param name="primaryKey">行的主键</param>
        /// <param name="timestamp">默认时间戳</param>
        public RowPutChange(String tableName, PrimaryKey primaryKey, long timestamp) : base(tableName, primaryKey)
        {
            this.timestamp = timestamp;
        }

        /// <summary>
        /// 拷贝构造函数
        /// </summary>
        /// <param name="toCopy">To copy.</param>
        public RowPutChange(RowPutChange toCopy) : base(toCopy.TableName, toCopy.GetPrimaryKey())
        {
            if (toCopy.timestamp.HasValue)
            {
                timestamp = toCopy.timestamp;
            }

            columnsToPut.AddRange(toCopy.columnsToPut);
        }

        /// <summary>
        /// 新写入一个属性列。
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="column">Column(for invocation chain).</param>
        public RowPutChange AddColumn(Column column)
        {
            this.columnsToPut.Add(column);
            return this;
        }

        /// <summary>
        /// 新写入一个属性列。
        /// <p>若设置过{@link #timestamp}，则使用该默认的时间戳。</p>
        /// </summary>
        /// <returns>The column(for invocation chain).</returns>
        /// <param name="name">属性列的名称</param>
        /// <param name="value">属性列的值</param>
        public RowPutChange AddColumn(String name, ColumnValue value)
        {
            Column column = null;
            if (this.timestamp.HasValue)
            {
                column = new Column(name, value, this.timestamp.Value);
            }
            else
            {
                column = new Column(name, value);
            }

            this.columnsToPut.Add(column);
            return this;
        }

        /// <summary>
        /// 新写入一个属性列。
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="name">属性列的名称</param>
        /// <param name="value">属性列的值</param>
        /// <param name="timestamp">T属性列的时间戳</param>
        public RowPutChange AddColumn(String name, ColumnValue value, long timestamp)
        {
            this.columnsToPut.Add(new Column(name, value, timestamp));
            return this;
        }

        /// <summary>
        /// 新写入一批属性列。
        /// <p>属性列的写入顺序与列表中的顺序一致。</p>
        /// </summary>
        /// <returns>The columns(for invocation chain).</returns>
        /// <param name="columns">属性列列表</param>
        public RowPutChange AddColumns(IEnumerable<Column> columns)
        {
            this.columnsToPut.AddRange(columns);
            return this;
        }

        /// <summary>
        /// 新写入一批属性列。
        /// <p>属性列的写入顺序与列表中的顺序一致。</p>
        /// </summary>
        /// <returns>The columns(for invocation chain).</returns>
        /// <param name="columns">属性列列表</param>
        public RowPutChange AddColumns(AttributeColumns columns)
        {
            foreach (var key in columns.Keys)
            {
                this.columnsToPut.Add(new Column(key, columns[key]));
            }

            return this;
        }

        /// <summary>
        /// 获取所有要写入的属性列列表。
        /// </summary>
        /// <returns>属性列列表</returns>
        public List<Column> GetColumnsToPut()
        {
            return this.columnsToPut;
        }

        /// <summary>
        /// 获取名称与指定名称相同的所有属性列的列表
        /// </summary>
        /// <returns>属性列名称</returns>
        /// <param name="name">若找到对应的属性列，则返回包含这些元素的列表，否则返回一个空列表。</param>
        public List<Column> GetColumnsToPut(string name)
        {
            List<Column> result = new List<Column>();

            foreach (Column col in columnsToPut)
            {
                if (col.Name.Equals(name))
                {
                    result.Add(col);
                }
            }

            return result;
        }

        public new int GetDataSize()
        {
            int valueTotalSize = 0;
            foreach (Column col in columnsToPut)
            {
                valueTotalSize += col.GetDataSize();
            }
            return GetPrimaryKey().GetDataSize() + valueTotalSize;
        }

        /// <summary>
        /// 检查是否已经有相同名称的属性列写入，忽略时间戳和值是否相等。
        /// </summary>
        /// <returns>若有返回true，否则返回false</returns>
        /// <param name="name">属性列名称</param>
        public bool Has(String name)
        {
            foreach (Column col in columnsToPut)
            {
                if (col.Name.Equals(name))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 检查是否有相同名称和相同时间戳的属性列写入，忽略值是否相等。
        /// </summary>
        /// <returns>若有返回true，否则返回false</returns>
        /// <param name="name">属性列名称</param>
        /// <param name="ts">属性列时间戳</param>
        public bool Has(String name, long ts)
        {
            foreach (Column col in columnsToPut)
            {
                if (col.Name.Equals(name) && (col.Timestamp.HasValue && col.Timestamp == ts))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 检查是否有相同名称和相同值的属性列写入，忽略时间戳是否相等。
        /// </summary>
        /// <returns>若有返回true，否则返回false</returns>
        /// <param name="name">属性列名称</param>
        /// <param name="value">属性列值</param>
        public bool Has(String name, ColumnValue value)
        {
            foreach (Column col in columnsToPut)
            {
                if (col.Name.Equals(name) && col.Value.Equals(value))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 检查是否有相同名称、相同时间戳并且相同值的属性列写入。
        /// </summary>
        /// <returns>若有返回true，否则返回false</returns>
        /// <param name="name">属性列名称</param>
        /// <param name="ts">属性列时间戳</param>
        /// <param name="value">属性列值</param>
        public bool Has(String name, long ts, ColumnValue value)
        {
            foreach (Column col in columnsToPut)
            {
                if (col.Name.Equals(name) && (col.Timestamp.HasValue && col.Timestamp == ts) &&
                        value.Equals(col.Value))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
