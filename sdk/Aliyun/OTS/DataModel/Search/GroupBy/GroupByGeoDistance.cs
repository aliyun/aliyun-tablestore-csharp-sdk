using System.Collections.Generic;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    /// <summary>
    /// 根据地理位置坐标进行分组
    /// </summary>
    public class GroupByGeoDistance : IGroupBy
    {
        private readonly GroupByType GroupByType = GroupByType.GroupByGeoDistance;

        public string GroupByName { get; set; }

        public string FieldName { get; set; }
        /// <summary>
        /// 设置起始中心点坐标
        /// </summary>
        public GeoPoint Origin { get; set; }
        /// <summary>
        /// 分组的依据范围
        /// </summary>
        public List<Range> Ranges { get; set; }
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
            return SearchGroupByBuilder.BuildGroupByGeoDistance(this).ToByteString();
        }
    }
}
