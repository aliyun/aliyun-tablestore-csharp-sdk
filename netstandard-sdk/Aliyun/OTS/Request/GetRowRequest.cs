/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 */

using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示一个GetRow请求。
    /// </summary>
    public class GetRowRequest : OTSRequest
    {
        /// <summary>
        /// 设置或获取过滤条件
        /// </summary>
        public SingleRowQueryCriteria QueryCriteria { get; private set; }

        /// <summary>
        /// 构造一个新的<see cref="GetRowRequest" />实例。
        /// </summary>
        /// <param name="singleRowQueryCriteria">条件</param>
        public GetRowRequest(SingleRowQueryCriteria singleRowQueryCriteria)
        {
            QueryCriteria = singleRowQueryCriteria;
        }

        /// <summary>
        /// 构造一个新的<see cref="GetRowRequest" />实例。
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="columnsToGet">获取的列名称列表，如果为空，则获取所有列</param>
        /// <param name="condition">过滤条件</param>
        public GetRowRequest(string tableName,
                             PrimaryKey primaryKey,
                             HashSet<string> columnsToGet = null,
                             IColumnCondition condition = null,
                             TimeRange timeRange = null,
                             int? maxVersion = null,
                             bool? cacheBlocks = null,
                             string startColumn = null,
                             string endColumn = null,
                             byte[] token = null
                            )
        {
            QueryCriteria = new SingleRowQueryCriteria(tableName)
            {
                RowPrimaryKey = primaryKey
            };

            if (columnsToGet != null)
            {
                QueryCriteria.SetColumnsToGet(columnsToGet);
            }

            if (condition != null)
            {
                QueryCriteria.Filter = condition.ToFilter();
            }

            if (timeRange != null)
            {
                QueryCriteria.TimeRange = timeRange;
            }

            QueryCriteria.MaxVersions = maxVersion;
            QueryCriteria.CacheBlocks = cacheBlocks;
            QueryCriteria.StartColumn = startColumn;
            QueryCriteria.EndColumn = endColumn;
            QueryCriteria.Token = token;
        }

        /// <summary>
        /// 主键
        /// </summary>
        [Obsolete("PrimaryKey is deprecated, please use QueryCriteria.RowPrimaryKey instead.")]
        public PrimaryKey PrimaryKey
        {
            get
            {
                return QueryCriteria.RowPrimaryKey;
            }

            set
            {
                QueryCriteria.RowPrimaryKey = value;
            }
        }

        /// <summary>
        /// 表名
        /// </summary>
        [Obsolete("TableName is deprecated, please use QueryCriteria.TableName instead.")]
        public string TableName
        {
            get
            {
                return QueryCriteria.TableName;
            }

            set
            {
                QueryCriteria.TableName = value;
            }
        }

        /// <summary>
        /// 要读取的列的列名；默认为读取所有的列。
        /// </summary>
        [Obsolete("ColumnsToGet is deprecated, please use QueryCriteria.GetColumnsToGet() or QueryCriteria.SetColumnsToGet instead.")]
        public HashSet<string> ColumnsToGet
        {
            get
            {
                return QueryCriteria.GetColumnsToGet();
            }

            set
            {
                QueryCriteria.SetColumnsToGet(value);
            }
        }
    }
}
