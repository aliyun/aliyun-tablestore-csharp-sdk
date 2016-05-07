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
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表示返回行的查询条件
    /// </summary>
    public class RowQueryCriteria
    {
        /// <summary>
        /// 查询的表的名称。
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// 要查询的列的名称，若未指定查询的列，则查询整行
        /// </summary>
        private HashSet<string> columnsToGet = new HashSet<string>();

        /// <summary>
        /// 本次查询使用的Filter
        /// </summary>
        public ColumnCondition Filter { get; set; }

        /// <summary>
        /// 构造一个在给定名称的表中查询的条件
        /// </summary>
        /// <param name="tableName">要查询的表名称</param>
        public RowQueryCriteria(string tableName)
        {
            TableName = tableName;
        }

        /// <summary>
        /// 获取返回列的名称的列表
        /// </summary>
        /// <returns>返回列的名称列表</returns>
        public HashSet<string> GetColumnsToGet()
        {
            return columnsToGet;
        }

        /// <summary>
        /// 添加要返回的列
        /// </summary>
        /// <param name="columnName">返回的列的名称</param>
        public void AddColumnsToGet(string columnName)
        {
            columnsToGet.Add(columnName);
        }

        /// <summary>
        /// 添加要返回的列。 
        /// </summary>
        /// <param name="columnNames">要返回列的名称</param>
        public void AddColumnsToGet(HashSet<string> columnNames)
        {
            foreach (string column in columnNames)
            {
                columnsToGet.Add(column);
            }
        }

        /// <summary>
        /// 设置需要读取的列的列表。若List为空，则读取所有列
        /// </summary>
        /// <param name="columnsToGet">需要读取的列的列表</param>
        public void SetColumnsToGet(HashSet<string> columnNames)
        {
            columnsToGet = columnNames;
        }
    }
}
