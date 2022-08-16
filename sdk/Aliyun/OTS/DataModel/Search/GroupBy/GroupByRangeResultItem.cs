using Aliyun.OTS.DataModel.Search.Agg;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByRangeResultItem
    {
        public double From { get; set; }

        public double To { get; set; }

        public long RowCount { get; set; }

        public AggregationResults SubAggregationResults { get; set; }

        public GroupByResults SubGroupByResults { get; set; }
    }
}
