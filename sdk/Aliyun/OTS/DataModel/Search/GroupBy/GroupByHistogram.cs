using System.Collections.Generic;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    /// <summary>
    /// 对某一个字段进行直方图统计。
    /// <p>举例：</p>
    /// <p>数据有 1、1、5、5、8、10，对这组数据进行直方图统计，间隔（interval）为5，返回： “0->2；5->3；10->1”这样的范围统计数据。</p>
    /// </summary>
    public class GroupByHistogram : IGroupBy
    {
        private readonly GroupByType GroupByType = GroupByType.GroupByHistogram;

        public string GroupByName { get; set; }

        public string FieldName { get; set; }
        /// <summary>
        /// 间隔
        /// </summary>
        public ColumnValue Interval { get; set; }
        /// <summary>
        /// 缺失字段的默认值。
        /// 如果一个文档缺少该字段，则采用什么默认值。
        /// </summary>
        public ColumnValue Missing { get; set; }
        /// <summary>
        /// 排序
        /// </summary>
        public List<GroupBySorter> GroupBySorters { get; set; }

        public long? MinDocCount { get; set; }
        /// <summary>
        /// 桶边界限制
        /// </summary>
        public FieldRange FieldRange { get; set; }
        /// <summary>
        /// 子聚合
        /// </summary>
        public List<IAggregation> SubAggregations { get; set; }
        /// <summary>
        /// 子分组
        /// </summary>
        public List<IGroupBy> SubGroupBys { get; set; }

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType;
        }

        public ByteString Serialize()
        {
            return SearchGroupByBuilder.BuildGroupByHistogram(this).ToByteString();
        }
    }
}
