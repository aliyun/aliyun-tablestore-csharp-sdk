namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class CountAggregationResult : IAggregationResult
    {
        public string AggName { get; set; }

        public long Value { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggCount;
        }
    }
}
