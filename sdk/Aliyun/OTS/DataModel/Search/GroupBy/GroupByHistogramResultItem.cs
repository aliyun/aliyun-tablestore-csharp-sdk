using Aliyun.OTS.DataModel.Search.Agg;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByHistogramResultItem
    {
        public ColumnValue Key { get; set; }

        public long Value { get; set; }

        public AggregationResults SubAggregationResults { get; set; }

        public GroupByResults SubGroupByResults { get; set; }
    }
}
