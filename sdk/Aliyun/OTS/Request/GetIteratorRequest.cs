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
    /// 表示一个GetIterator请求。
    /// </summary>
    public class GetIteratorRequest : GetRangeRequest
    {
        public CapacityUnit ConsumedCapacityUnitCounter { get; private set; }

        /// <summary>
        /// 通过<see cref="RangeRowQueryCriteria"/>构造一个新的<see cref="GetIteratorRequest"/>
        /// </summary>
        /// <param name="queryCriteria"></param>
        public GetIteratorRequest(RangeRowQueryCriteria queryCriteria,
                                  CapacityUnit consumedCapacityUnitCounter)
            : base(queryCriteria)
        {
            ConsumedCapacityUnitCounter = consumedCapacityUnitCounter;
        }

        /// <summary>
        /// 通过多个参数构造一个新的<see cref="GetRangeRequest"/>
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="direction">前向还是后向</param>
        /// <param name="inclusiveStartPrimaryKey">区间开始位置，包含</param>
        /// <param name="exclusiveEndPrimaryKey">区间结束位置，不包含</param>
        /// <param name="consumedCapacityUnitCounter">用户传入的CapacityUnit消耗计数器。</param>
        /// <param name="columnsToGet">返回的列名称的列表</param>
        /// <param name="limit">最大返回数</param>
        public GetIteratorRequest(string tableName,
                                 GetRangeDirection direction,
                                 PrimaryKey inclusiveStartPrimaryKey,
                                 PrimaryKey exclusiveEndPrimaryKey,
                                 CapacityUnit consumedCapacityUnitCounter,
                                 HashSet<string> columnsToGet = null,
                                 int? limit = null,
                                 ColumnCondition condition = null)
            : base (tableName, direction, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 
                    columnsToGet, limit, condition)
        {
            ConsumedCapacityUnitCounter = consumedCapacityUnitCounter;
        }
    }
}
