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
using System.Collections.Generic;
using Aliyun.OTS.Util;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表示若干主键列组成的主键。可以使用Add方法指定列名和列值来添加主键列。
    /// </summary>
    public class PrimaryKey : Dictionary<string, ColumnValue>, IComparable<PrimaryKey>, IMeasurable
    {
        /// <summary>
        /// 获取所有的主键列。
        ///  <p>主键中包含的主键列的个数以及主键列的顺序与创建表时TableMeta中定义的一致。</p>
        /// </summary>
        /// <returns>The primary key columns.</returns>
        public PrimaryKeyColumn[] GetPrimaryKeyColumns()
        {
            if (this == null)
            {
                return null;
            }

            PrimaryKeyColumn[] keyColumns = new PrimaryKeyColumn[this.Count];

            var enumerator = this.GetEnumerator();

            for (var i = 0; enumerator.MoveNext();i++)
            {
                keyColumns[i] = new PrimaryKeyColumn(enumerator.Current);
            }

            return keyColumns;
        }

        /// <summary>
        /// 获取行主键的数据大小总和，大小总和包括所有主键列的名称和值。
        /// </summary>
        /// <returns>行主键的数据大小总和</returns>
        public int GetDataSize()
        {
            if (this.Keys == null)
            {
                return 0;
            }


            int size = 0;
            foreach (var key in this.Keys)
            {
                size += OtsUtils.CalcStringSizeInBytes(key);
                size += this[key].GetDataSize();
            }

            return size;
        }

        public override string ToString()
        {
            var primaryColumns = GetPrimaryKeyColumns();
            string result = "";

            if(primaryColumns == null)
            {
                return result;
            }

            foreach(var col in primaryColumns)
            {
                result += col + ",";
            }

            return result;
        }

        /// <summary>
        /// 比较两个主键
        /// <p>对比的两个主键必须为相同的schema，即列数、主键名称和顺序都完全一致。</p>
        /// </summary>
        /// <returns>比较结果</returns>
        /// <param name="target">Target.</param>
        public int CompareTo(PrimaryKey target)
        {
            if (this.Keys.Count != target.Keys.Count)
            {
                throw new ArgumentException("The schema of the two primary key compared is not the same.");
            }

            for (int i = 0; i < this.Count; i++)
            {
                int ret = string.Compare(this.Keys.GetEnumerator().Current, target.Keys.GetEnumerator().Current, StringComparison.Ordinal);
                if (ret != 0)
                {
                    return ret;
                }
            }

            return 0;
        }
    }
}
