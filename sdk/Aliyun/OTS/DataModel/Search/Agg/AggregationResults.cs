using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class AggregationResults
    {
        public Dictionary<string, IAggregationResult> ResultMap;

        public int Size()
        {
            if (ResultMap == null)
            {
                return 0;
            }
            return ResultMap.Count;
        }

        public AggregationResults()
        {
        }

        public AggregationResults(Dictionary<string, IAggregationResult> resultMap)
        {
            if (resultMap != null && resultMap.Count != 0)
            {
                ResultMap = resultMap;
            }
        }

        public AvgAggregationResult GetAsAvgAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggAvg)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to AvgAggregationResult.", aggregationName));
            }

            return (AvgAggregationResult)result;
        }

        public DistinctCountAggregationResult GetAsDistinctCountAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggDistinctCount)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to DistinctCountAggregationResult.", aggregationName));
            }

            return (DistinctCountAggregationResult)result;
        }

        public MaxAggregationResult GetAsMaxAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggMax)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to MaxAggregationResult.", aggregationName));
            }

            return (MaxAggregationResult)result;
        }

        public MinAggregationResult GetAsMinAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggMin)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to MinAggregationResult.", aggregationName));
            }

            return (MinAggregationResult)result;
        }

        public SumAggregationResult GetAsSumAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggSum)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to SumAggregationResult.", aggregationName));
            }

            return (SumAggregationResult)result;
        }

        public CountAggregationResult GetAsCountAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggCount)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to CountAggregationResult.", aggregationName));
            }

            return (CountAggregationResult)result;
        }

        public TopRowsAggregationResult GetAsTopRowsAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggTopRows)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to TopRowsAggregationResult.", aggregationName));
            }

            return (TopRowsAggregationResult)result;
        }

        public PercentilesAggregationResult GetAsPercentilesAggregationResult(string aggregationName)
        {
            if (ResultMap == null || !ResultMap.ContainsKey(aggregationName))
            {
                throw new ArgumentException(string.Format("AggregationResults " +
                                                          "do not contains: {0}", aggregationName));
            }

            IAggregationResult result = ResultMap[aggregationName];

            if (result.GetAggType() != AggregationType.AggPercentiles)
            {
                throw new ArgumentException(string.Format("the result with this aggregationName: [{0}] can't cast to PercentilesAggregationResult.", aggregationName));
            }

            return (PercentilesAggregationResult)result;
        }
    }
}
