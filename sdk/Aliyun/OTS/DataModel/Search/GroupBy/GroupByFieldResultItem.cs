using Aliyun.OTS.DataModel.Search.Agg;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByFieldResultItem
    {
        public string Key { get; set; }

        public long RowCount { get; set; }

        public AggregationResults SubAggregationResults { get; set; }

        public GroupByResults SubGroupByResults { get; set; }
    }
}
