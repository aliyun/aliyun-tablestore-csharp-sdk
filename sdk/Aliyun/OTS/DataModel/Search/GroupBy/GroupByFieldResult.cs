using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByFieldResult : IGroupByResult
    {
        public string GroupByName { get; set; }

        public List<GroupByFieldResultItem> GroupByFieldResultItems;

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType.GroupByField;
        }
    }
}
