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

using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表示若干属性列组成的属性。可以使用Add方法指定列名和列值来添加属性列。
    /// </summary>
    public class AttributeColumns : Dictionary<string, ColumnValue>
    {
        /// <summary>
        /// 某些列会有多个版本的数据，需要把这些数据保存下来
        /// </summary>
        private readonly Dictionary<string, List<ColumnValue>> multiVersionColumns = new Dictionary<string, List<ColumnValue>>();

        public static Dictionary<string, ColumnValue> ParseColumnArray(Column[] columns)
        {
            AttributeColumns keyValuePairs = new AttributeColumns();
            foreach(var column in columns)
            {
                keyValuePairs.Add(column);
            }

            return keyValuePairs;
        }

        public void Add(Column column)
        {
            if (!this.ContainsKey(column.Name))
            {
                this.Add(column.Name, column.Value);
            }
            else
            {
                if(!this.multiVersionColumns.ContainsKey(column.Name))
                {
                    this.multiVersionColumns.Add(column.Name, new List<ColumnValue>());
                }

                this.multiVersionColumns[column.Name].Add(column.Value);
            }
        }

        /// <summary>
        /// 得到某一列的多个版本的值
        /// </summary>
        /// <returns>The all column values.</returns>
        /// <param name="columnName">Column name.</param>
        public List<ColumnValue> GetAllColumnValues(string columnName)
        {
            List<ColumnValue> columnValues = new List<ColumnValue>();
            if(this.ContainsKey(columnName))
            {
                columnValues.Add(this[columnName]);
            }

            if(this.multiVersionColumns.ContainsKey(columnName))
            {
                columnValues.AddRange(this.multiVersionColumns[columnName]);
            }

            return columnValues;
        }

        /// <summary>
        /// 得到所有返回值的版本
        /// </summary>
        /// <returns>The all attribute columns.</returns>
        public Dictionary<string, List<ColumnValue>> GetAllAttributeColumns()
        {
            Dictionary<string, List<ColumnValue>> attributeColumns = new Dictionary<string, List<ColumnValue>>();

            foreach(var key in this.Keys)
            {
                if(attributeColumns.ContainsKey(key))
                {
                    attributeColumns[key].AddRange(this.multiVersionColumns[key]);
                }
                else
                {
                    attributeColumns.Add(key, new List<ColumnValue>());
                    attributeColumns[key].Add(this[key]);
                }
            }

            return attributeColumns;
        }
    }
}
