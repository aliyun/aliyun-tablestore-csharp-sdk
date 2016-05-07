/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 */

using System.Collections.Generic;
using System.Linq;

namespace Aliyun.OTS.DataModel
{
    public class MultiRowQueryCriteria : RowQueryCriteria
    {
        private List<PrimaryKey> rowPrimaryKeys = new List<PrimaryKey>();

        /// <summary>
        /// 构造一个在给定名称的表中查询的条件
        /// </summary>
        /// <param name="tableName"></param>
        public MultiRowQueryCriteria(string tableName)
            : base(tableName)
        {}

        /// <summary>
        /// 向多行查询条件中插入要查询的行的主键
        /// </summary>
        /// <param name="primaryKey">要查询的行的主键</param>
        public void AddRowKey(PrimaryKey primaryKey)
        {
            rowPrimaryKeys.Add(primaryKey);
        }

        /// <summary>
        /// 设置该表中所有要查询的行的主键
        /// </summary>
        /// <param name="primaryKeys">所有行的主键</param>
        public void SetRowKeys(List<PrimaryKey> primaryKeys)
        {
            rowPrimaryKeys = primaryKeys;
        }

        /// <summary>
        /// 获取该表中所要要查询的行的主键。
        /// </summary>
        /// <returns>所有行的主键。</returns>
        public List<PrimaryKey> GetRowKeys()
        {
            return rowPrimaryKeys;
        }

        /// <summary>
        ///  获取某行的主键,若该行index不存在，则返回null。
        /// </summary>
        /// <param name="index">该行的索引</param>
        /// <returns>若该行存在，则返回该行主键，否则返回null</returns>
        public PrimaryKey Get(int index)
        {
            if (index < rowPrimaryKeys.Count())
            {
                return rowPrimaryKeys[index];
            }
            return null;
        }

        /// <summary>
        /// 清空要查询的所有行
        /// </summary>
        public void Clear()
        {
            rowPrimaryKeys.Clear();
        }
    }
}
