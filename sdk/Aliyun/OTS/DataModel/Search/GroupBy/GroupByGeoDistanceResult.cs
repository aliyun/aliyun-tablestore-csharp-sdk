using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByGeoDistanceResult : IGroupByResult
    {
        public string GroupByName { get; set; }

        public List<GroupByGeoDistanceResultItem> GroupByGeoDistanceResultItems;

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType.GroupByGeoDistance;
        }
    }
}
