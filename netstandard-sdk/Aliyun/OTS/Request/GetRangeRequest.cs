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
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示GetRange的方向的枚举类型，向前或者向后。
    /// </summary>
    public enum GetRangeDirection
    {
        /// <summary>
        /// 向前
        /// </summary>
        Forward,
        
        /// <summary>
        /// 向后
        /// </summary>
        Backward,
    }

    /// <summary>
    /// 表示一个GetRange请求。
    /// </summary>
    public class GetRangeRequest : OTSRequest
    {
        /// <summary>
        /// 设置或获取<see cref="RangeRowQueryCriteria"/>
        /// </summary>
        public RangeRowQueryCriteria QueryCriteria { get; set; }

        /// <summary>
        /// 通过<see cref="RangeRowQueryCriteria"/>构造一个新的<see cref="GetRangeRequest"/>
        /// </summary>
        /// <param name="queryCriteria"></param>
        public GetRangeRequest(RangeRowQueryCriteria queryCriteria)
        {
            QueryCriteria = queryCriteria;
        }

        /// <summary>
        /// 通过多个参数构造一个新的<see cref="GetRangeRequest"/>
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="direction">前向还是后向</param>
        /// <param name="inclusiveStartPrimaryKey">区间开始位置，包含</param>
        /// <param name="exclusiveEndPrimaryKey">区间结束位置，不包含</param>
        /// <param name="columnsToGet">返回的列名称的列表</param>
        /// <param name="limit">最大返回数</param>
        public GetRangeRequest(string tableName,
                               GetRangeDirection direction,
                               PrimaryKey inclusiveStartPrimaryKey,
                               PrimaryKey exclusiveEndPrimaryKey,
                               HashSet<string> columnsToGet = null,
                               int? limit = null,
                               IColumnCondition condition = null)
        {
            QueryCriteria = new RangeRowQueryCriteria(tableName)
            {
                Direction = direction,
                Limit = limit,
                InclusiveStartPrimaryKey = inclusiveStartPrimaryKey,
                ExclusiveEndPrimaryKey = exclusiveEndPrimaryKey
            };

            if (columnsToGet != null)
            {
                QueryCriteria.SetColumnsToGet(columnsToGet);
            }

            if (condition != null)
            {
                QueryCriteria.Filter = condition.ToFilter();
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
        /// 范围的方向，向前<see cref="GetRangeDirection.Forward"/>或者向后<see cref="GetRangeDirection.Backward"/>
        /// </summary>
        [Obsolete("Direction is deprecated, please use QueryCriteria.Direction instead.")]
        public GetRangeDirection Direction
        {
            get
            {
                return QueryCriteria.Direction;
            }

            set
            {
                QueryCriteria.Direction = value;
            }
        }

        /// <summary>
        /// 起始主键，包含。这里的主键列可以用<see cref="ColumnValue.INF_MIN"/>或<see cref="ColumnValue.INF_MAX"/>表示最小值或者最大值
        /// </summary>        [Obsolete("InclusiveStartPrimaryKey is deprecated, please use QueryCriteria.InclusiveStartPrimaryKey instead.")]        public PrimaryKey InclusiveStartPrimaryKey
        {
            get
            {
                return QueryCriteria.InclusiveStartPrimaryKey;
            }
            set
            {
                QueryCriteria.InclusiveStartPrimaryKey = value;
            }

        }

        /// <summary>
        /// 结束主键，不包含。这里的主键列可以用<see cref="ColumnValue.INF_MIN"/>或<see cref="ColumnValue.INF_MAX"/>表示最小值或者最大值
        /// </summary>
        [Obsolete("ExclusiveEndPrimaryKey is deprecated, please use QueryCriteria.ExclusiveEndPrimaryKey instead.")]
        public PrimaryKey ExclusiveEndPrimaryKey
        {
            get
            {
                return QueryCriteria.ExclusiveEndPrimaryKey;
            }
            set
            {
                QueryCriteria.ExclusiveEndPrimaryKey = value;
            }
        }

        /// <summary>
        /// 每行要读取的列的列名；默认读取所有的列
        /// </summary>
        [Obsolete("ColumnsToGet is deprecated, please use QueryCriteria.GetColumnsToGet() or QueryCriteria.SetColumnsToGet(value) instead.")]
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

        /// <summary>
        /// 最多读取多少行；默认读取范围内所有的行
        /// </summary>
        [Obsolete("Limit is deprecated, please use QueryCriteria.Limit instead.")]
        public int? Limit
        {
            get
            {
                return QueryCriteria.Limit;
            }
            set
            {
                QueryCriteria.Limit = value;
            }
        }

    }
}
