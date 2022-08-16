using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.Sort;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    /// <summary>
    /// 对某一个字段进行分组统计
    /// 举例：库存账单里有"篮球"、"足球"、"羽毛球"等，对这一个字段进行聚合，返回："篮球：10个"、"足球：5个"、"羽毛球：1个"诸如此类的聚合信息。
    /// </summary>
    public class GroupByField : IGroupBy
    {
        public readonly GroupByType GroupbyType = GroupByType.GroupByField;
        /// <summary>
        /// GroupBy名称，可从GroupBy结果列表中根据该名字获取GroupBy结果
        /// </summary>
        public string GroupByName { get; set; }
        /// <summary>
        /// 字段名称
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        /// 返回分组数量
        /// </summary>
        public int? Size { get; set; }
        /// <summary>
        /// 排序
        /// </summary>
        public List<GroupBySorter> GroupBySorters { get; set; }
        /// <summary>
        /// 子聚合
        /// </summary>
        public List<IAggregation> SubAggregations { get; set; }
        /// <summary>
        /// 子分组
        /// </summary>
        public List<IGroupBy> SubGroupBys { get; set; }
        /// <summary>
        /// 最小文档数
        /// </summary>
        public long? MinDocCount { get; set; }

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupbyType;
        }

        public ByteString Serialize()
        {
            return SearchGroupByBuilder.BuildGroupByField(this).ToByteString();
        }
    }
}
