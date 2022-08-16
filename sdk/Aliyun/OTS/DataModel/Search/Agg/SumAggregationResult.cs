namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class SumAggregationResult : IAggregationResult
    {
        public string AggName { get; set; }

        public double Value { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggSum;
        }
    }
}
