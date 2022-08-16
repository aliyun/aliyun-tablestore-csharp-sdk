namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class AvgAggregationResult : IAggregationResult
    {
        /// <summary>
        /// 聚合的名字
        /// </summary>
        public string AggName { get; set; }
        /// <summary>
        /// 聚合的结果
        /// </summary>
        public double Value { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggAvg;
        }
    }
}
