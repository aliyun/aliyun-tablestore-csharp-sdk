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
using Aliyun.OTS.DataModel.Filter;

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
        ///  要读取的时间戳的范围，若未设置，则代表读取所有的版本。
        /// </summary>
        public TimeRange TimeRange { get; set; }

        /// <summary>
        /// 要返回的列的版本的个数，若未设置，则返回OTS当前保留的所有版本。
        /// </summary>
        public int? MaxVersions { get; set; }

        /// <summary>
        /// 本次查询使用的Filter
        /// </summary>
        public IFilter Filter { get; set; }

        /// <summary>
        /// 查询的列范围的起始位置.
        /// </summary>
        public string StartColumn { get; set; }

        /// <summary>
        /// 查询的列范围的终止位置.
        /// </summary>
        public string EndColumn { get; set; }

        /// <summary>
        /// 内部参数。
        /// </summary>
        public bool? CacheBlocks { get; set; }


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
        /// <param name="columnNames">需要读取的列的列表</param>
        public void SetColumnsToGet(HashSet<string> columnNames)
        {
            this.columnsToGet = columnNames;
        }

        public void CopyTo(RowQueryCriteria target)
        {
            target.TableName = TableName;
            target.columnsToGet.UnionWith(this.columnsToGet);
            target.TimeRange = TimeRange;
            target.MaxVersions = MaxVersions;
            target.CacheBlocks = CacheBlocks;
            target.Filter = Filter;
            target.StartColumn = StartColumn;
            target.EndColumn = EndColumn;
        }
    }
}
