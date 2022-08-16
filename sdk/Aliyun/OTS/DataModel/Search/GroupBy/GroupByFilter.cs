using System.Collections.Generic;
using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    /// <summary>
    /// 根据查询语句进行分组。
    /// </summary>
    public class GroupByFilter : IGroupBy
    {
        private readonly GroupByType GroupByType = GroupByType.GroupByFilter;

        public string GroupByName { get; set; }

        public List<IQuery> Filters { get; set; }

        public List<IAggregation> SubAggregations { get; set; }

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
            return SearchGroupByBuilder.BuildGroupByFilter(this).ToByteString();
        }
    }
}
