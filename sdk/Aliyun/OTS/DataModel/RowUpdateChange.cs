using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{  
    public class RowUpdateChange : RowChange
    {

        /// <summary>
        ///
        ///所有要更新的属性列。
        ///<p>若类型为{@link Type#PUT}，则代表写入一个属性列。</p>
        ///<p>若类型为{@link Type#DELETE}，则代表删除一个属性列的某个特定版本，对应的Column中的value无效。</p>
        ///<p>若类型为{@link Type#DELETE_ALL}，则代表删除一个属性列的所有版本，对应的Column中的value和timestamp均无效。</p>
        ///
        /// </summary>
        private readonly List<Tuple<Column, RowChangeType>> columnsToUpdate = new List<Tuple<Column, RowChangeType>>();

        private long? timestamp;

        public RowUpdateChange(String tableName) : base(tableName)
        {

        }

        public RowUpdateChange(String tableName, PrimaryKey primaryKey) : base(tableName, primaryKey)
        {

        }

        public RowUpdateChange(String tableName, PrimaryKey primaryKey, long ts) : base(tableName, primaryKey)
        {
            this.timestamp = ts;
        }

        public RowUpdateChange(RowUpdateChange toCopy) : base(toCopy.TableName, toCopy.PrimaryKey)
        {
            if (toCopy.timestamp.HasValue)
            {
                timestamp = toCopy.timestamp;
            }

            columnsToUpdate.AddRange(toCopy.columnsToUpdate);
        }

        /// <summary>
        /// 新写入一个属性列。
        /// </summary>
        /// <returns>The put.</returns>
        /// <param name="column">Column.</param>
        public RowUpdateChange Put(Column column)
        {
            this.columnsToUpdate.Add(new Tuple<Column, RowChangeType>(column, RowChangeType.PUT));
            return this;
        }

        /// <summary>
        /// 新写入一个属性列。
        /// <p>若设置过{@link #timestamp}，则使用该默认的时间戳。</p>
        /// </summary>
        /// <returns>The put.</returns>
        /// <param name="name">属性列的名称</param>
        /// <param name="value">属性列的值</param>
        public RowUpdateChange Put(String name, ColumnValue value)
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

            this.columnsToUpdate.Add(new Tuple<Column, RowChangeType>(column, RowChangeType.PUT));
            return this;
        }

        /// <summary>
        /// 新写入一个属性列。
        /// </summary>
        /// <returns>The put.</returns>
        /// <param name="name">属性列的名称</param>
        /// <param name="value">属性列的值</param>
        /// <param name="ts">属性列的时间戳</param>
        public RowUpdateChange Put(String name, ColumnValue value, long ts)
        {
            this.columnsToUpdate.Add(new Tuple<Column, RowChangeType>(new Column(name, value, ts), RowChangeType.PUT));
            return this;
        }

        /// <summary>
        /// 新写入一批属性列。
        /// <p>属性列的写入顺序与列表中的顺序一致。</p>
        /// </summary>
        /// <returns>The put.</returns>
        /// <param name="columns">属性列列表</param>
        public RowUpdateChange Put(List<Column> columns)
        {
            foreach (Column col in columns)
            {
                Put(col);
            }

            return this;
        }

        /// <summary>
        /// 删除某一属性列的特定版本。
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="name">属性列的名称</param>
        /// <param name="ts">属性列的时间戳</param>
        public RowUpdateChange DeleteColumn(String name, long ts)
        {
            this.columnsToUpdate.Add(new Tuple<Column, RowChangeType>(new Column(name, null, ts), RowChangeType.DELETE));
            return this;
        }

        /// <summary>
        /// 删除某一属性列的所有版本。
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="name">属性列的名称</param>
        public RowUpdateChange DeleteColumn(String name)
        {
            this.columnsToUpdate.Add(new Tuple<Column, RowChangeType>(new Column(name, null), RowChangeType.DELETE_ALL));
            return this;
        }

        /// <summary>
        /// 
        ///所有要更新的属性列。
        ///<p>若类型为{@link Type#PUT}，则代表写入一个属性列。</p>
        ///<p>若类型为{@link Type#DELETE}，则代表删除一个属性列的某个特定版本，对应的Column中的value无效。</p>
        ///<p>若类型为{@link Type#DELETE_ALL}，则代表删除一个属性列的所有版本，对应的Column中的value和timestamp均无效。</p>
        ///
        /// </summary>
        /// <returns>所有要更新的列</returns>
        public List<Tuple<Column, RowChangeType>> GetColumnsToUpdate()
        {
            return this.columnsToUpdate;
        }


        public new int GetDataSize()
        {
            int valueTotalSize = 0;
            foreach (Tuple<Column, RowChangeType> col in columnsToUpdate)
            {
                valueTotalSize += col.Item1.GetDataSize();
            }

            return GetPrimaryKey().GetDataSize() + valueTotalSize;
        }


        public RowUpdateChange FromUpdateOfAtrribute(UpdateOfAttribute updateOfAttribute)
        {
            if(updateOfAttribute.AttributeColumnsToDelete != null)
            {
                foreach(var attributeColumnToDelete in updateOfAttribute.AttributeColumnsToDelete)
                {
                    this.DeleteColumn(attributeColumnToDelete);
                }
            }

            if(updateOfAttribute.AttributeColumnsToPut != null)
            {
                foreach (var attributeColumnToPut in updateOfAttribute.AttributeColumnsToPut)
                {
                    this.Put(attributeColumnToPut.Key, attributeColumnToPut.Value);
                }
            }

            return this;
        }
    }
}
