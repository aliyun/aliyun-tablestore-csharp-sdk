using System.Collections.Generic;
using System.Text;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class PercentilesAggregationResult : IAggregationResult
    {
        public string AggName { get; set; }

        public List<PercentilesAggregationResultItem> Value { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType.AggPercentiles;
        }

        public string Serialize()
        {
            if (Value == null || Value.Count == 0)
            {
                return null;
            }

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < Value.Count; i++)
            {
                sb.Append(Value[i].Serialize());
                if (i != Value.Count - 1)
                {
                    sb.Append("\n");
                }
            }

            return sb.ToString();
        }
    }
}
