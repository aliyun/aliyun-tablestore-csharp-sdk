using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByRangeResult : IGroupByResult
    {
        public string GroupByName { get; set; }

        public List<GroupByRangeResultItem> GroupByRangeResultItems;

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType.GroupByRange;
        }
    }
}
