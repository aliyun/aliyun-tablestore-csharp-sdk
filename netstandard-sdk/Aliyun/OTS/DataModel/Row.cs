using System;
using System.Collections.Generic;
using System.Text;

namespace Aliyun.OTS.DataModel
{
    public class Row : IRow
    {
        public PrimaryKey PrimaryKey { get; set; }

        public AttributeColumns AttributeColumns{
            get{
                if(columns == null)
                {
                    return null;
                }

                var attributeColumns = new AttributeColumns();

                foreach(var column in columns)
                {
                    attributeColumns.Add(column);
                }

                return attributeColumns;
            }
        }

        private readonly Column[] columns;

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="primaryKey">行的主键，不能为null或者为空</param>
        /// <param name="columns">该行的属性列，不能为null</param>
        public Row(PrimaryKey primaryKey, List<Column> columns) : this(primaryKey, columns.ToArray())
        {

        }

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="primaryKey">行的主键，不能为null或者为空</param>
        /// <param name="columns">该行的属性列，不能为null</param>
        public Row(PrimaryKey primaryKey, Column[] columns)
        {
            PrimaryKey = primaryKey;
            this.columns = columns;
            SortColumns(); // it may not been sorted, so we should sort it first
        }

        public PrimaryKey GetPrimaryKey()
        {
            return PrimaryKey;
        }

        /// <summary>
        /// 获取所有的属性列。数组中的所有属性列按名称升序排列，相同名称的属性列按timestamp降序排列。
        /// </summary>
        /// <returns> 所有属性列</returns>
        public Column[] GetColumns()
        {
            return columns;
        }

        /// <summary>
        /// 获取某个特定名称的属性列的所有版本的值。返回结果中这些属性列按timestamp降序排列.
        /// </summary>
        /// <returns>若该属性列存在，则返回所有版本的值，按timestamp降序排列，否则返回空列表</returns>
        /// <param name="columnName">属性列的名称</param>
        public List<Column> GetColumn(String columnName)
        {
            List<Column> result = new List<Column>();

            if (columns == null || columns.Length == 0)
            {
                return result;
            }

            int pos = BinarySearch(columnName);
            if (pos == -1)
            {
                return result;
            }

            for (int i = pos; i < columns.Length; i++)
            {
                Column col = columns[i];
                if (col.Name.Equals(columnName))
                {
                    result.Add(col);
                }
                else
                {
                    break;
                }
            }

            return result;
        }

        /// <summary>
        /// 获取该属性列中最新版本的值。
        /// </summary>
        /// <returns>若该属性列存在，则返回最新版本的值，否则返回null</returns>
        /// <param name="name">属性列的名称</param>
        public Column GetLatestColumn(String name)
        {
            if (columns == null || columns.Length == 0)
            {
                return null;
            }

            int pos = BinarySearch(name);
            if (pos == -1)
            {
                return null;
            }

            Column col = columns[pos];

            if (col.Name.Equals(name))
            {
                return col;
            }

            return null;
        }

        /// <summary>
        /// 检查该行中是否有该名称的属性列。
        /// </summary>
        /// <returns>若存在，则返回true，否则返回false</returns>
        /// <param name="name">Name.</param>
        public bool Contains(String name)
        {
            return GetLatestColumn(name) != null;
        }

        /// <summary>
        /// 检查该行是否包含属性列。
        /// </summary>
        /// <returns>若该行不包含任何属性列，则返回true，否则返回false</returns>
        public bool IsEmpty()
        {
            return columns == null || columns.Length == 0;
        }

        public int CompareTo(IRow o)
        {

            return this.PrimaryKey.CompareTo(o.GetPrimaryKey());
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[PrimaryKey]:");
            sb.Append(this.PrimaryKey);
            sb.Append("\n[Columns]:");
            foreach (Column column in this.GetColumns())
            {
                sb.Append("(");
                sb.Append(column);
                sb.Append(")");
            }

            return sb.ToString();
        }

        /// <summary>
        /// 将数组中的所有属性列按名称升序、timestamp降序的顺序重新排列。
        /// </summary>
        private void SortColumns()
        {
            // check if it is already sorted, optimized as in most time it is sorted.
            bool sorted = true;
            for (int i = 0; i < columns.Length - 1; i++)
            {
                int ret = Column.NAME_TIMESTAMP_COMPARATOR.Compare(columns[i], columns[i + 1]);
                if (ret > 0)
                {
                    sorted = false;
                    break;
                }
            }

            if (!sorted)
            {
                Array.Sort(this.columns, Column.NAME_TIMESTAMP_COMPARATOR);
            }
        }

        /// <summary>
        /// 二分查找指定的列.
        /// </summary>
        /// <returns>T如果包含查找的列, 返回对应的index; 如果不包含该列, 返回可以插入该列的位置; 如果所有元素都小于该列, 返回-1.</returns>
        /// <param name="name">要查找的列名</param>
        private int BinarySearch(String name)
        {
            Column searchTerm = new Column(name, null, long.MaxValue);

            // 若数组中有多列与searchTerm相同，那不保证一定返回第一列，Row中的数据是TableStore返回的，不会出现这种情况。
            // pos === ( -(insertion point) - 1)
            int pos = Array.BinarySearch(columns, searchTerm, Column.NAME_TIMESTAMP_COMPARATOR);

            if (pos < 0)
            {
                pos = (pos + 1) * -1;
            }

            if (pos == columns.Length)
            {
                return -1;
            }

            return pos;
        }
    }
}
