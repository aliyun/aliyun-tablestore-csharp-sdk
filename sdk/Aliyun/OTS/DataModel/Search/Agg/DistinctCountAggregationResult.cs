namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class DistinctCountAggregationResult : IAggregationResult
    {
        public string AggName { get; set; }

        public long Value { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggDistinctCount;
        }
    }
}
