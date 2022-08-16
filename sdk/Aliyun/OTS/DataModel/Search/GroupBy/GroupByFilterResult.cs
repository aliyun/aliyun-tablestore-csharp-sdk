using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByFilterResult : IGroupByResult
    {
        public string GroupByName { get; set; }

        public List<GroupByFilterResultItem> GroupByFilterResultItems;

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType.GroupByFilter;
        }
    }
}
