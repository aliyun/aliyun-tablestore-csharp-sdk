using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByHistogramResult : IGroupByResult
    {
        public string GroupByName { get; set; }

        public List<GroupByHistogramResultItem> GroupByHistogramResultItems;

        public string GetGroupByName()
        {
            return GroupByName;
        }

        public GroupByType GetGroupByType()
        {
            return GroupByType.GroupByHistogram;
        }
    }
}
