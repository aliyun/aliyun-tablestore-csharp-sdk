using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class TopRowsAggregationResult : IAggregationResult
    {
        public string AggName { get; set; }

        public List<Row> Rows { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggTopRows;
        }
    }
}
