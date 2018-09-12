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
    /// <summary>
    ///从TableStore表中查询多行数据所需的参数，可以支持以下几种读取行为:
    ///<ul>
    ///<li>读取某些列或所有列的某个特定版本</li>
    ///<li>读取某些列或所有列的某个版本范围内的所有版本或最大的N个版本</li>
    ///<li>读取某些列或所有列的最大的N个版本(N最小为 1,最大为MaxVersions)</li>
    ///</ul>
    ///<p>注意：读取参数不能为每行单独设置，多行必须使用相同的查询参数。</p>
    ///
    /// </summary>
    public class MultiRowQueryCriteria : RowQueryCriteria
    {
        private List<PrimaryKey> rowPrimaryKeys = new List<PrimaryKey>();
        private List<byte[]> tokens;

        /// <summary>
        /// 构造一个在给定名称的表中查询的条件
        /// </summary>
        /// <param name="tableName"></param>
        public MultiRowQueryCriteria(string tableName)
            : base(tableName)
        {
            rowPrimaryKeys = new List<PrimaryKey>();
            tokens = new List<byte[]>();
        }

        /// <summary>
        /// 向多行查询条件中插入要查询的行的主键
        /// </summary>
        /// <param name="primaryKey">要查询的行的主键</param>
        public void AddRowKey(PrimaryKey primaryKey)
        {
            AddRowKey(primaryKey, new byte[0]);
        }

        public void AddRowKey(PrimaryKey primaryKey, byte[] token)
        {
            this.rowPrimaryKeys.Add(primaryKey);
            this.tokens.Add(token);
        }

        /// <summary>
        /// 设置该表中所有要查询的行的主键
        /// </summary>
        /// <param name="primaryKeys">所有行的主键</param>
        public void SetRowKeys(List<PrimaryKey> primaryKeys)
        {
            rowPrimaryKeys = primaryKeys;
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                tokens.Add(new byte[0]);
            }
        }

        /// <summary>
        /// 获取该表中所要要查询的行的主键。
        /// </summary>
        /// <returns>所有行的主键。</returns>
        public List<PrimaryKey> GetRowKeys()
        {
            return rowPrimaryKeys;
        }

        public List<byte[]> GetTokens()
        {
            return tokens;
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

        /// <summary>
        /// 获取要查询的行的个数。
        /// </summary>
        /// <returns>The size.</returns>
        public int Size()
        {
            return rowPrimaryKeys.Count;
        }

        public bool IsEmpty()
        {
            return rowPrimaryKeys.Count == 0;
        }

        public MultiRowQueryCriteria CloneWithoutRowKeys()
        {
            MultiRowQueryCriteria newCriteria = new MultiRowQueryCriteria(this.TableName);
            this.CopyTo(newCriteria);
            return newCriteria;
        }
    }
}
