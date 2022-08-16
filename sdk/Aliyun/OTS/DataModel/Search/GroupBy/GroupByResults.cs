using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class GroupByResults
    {
        public Dictionary<string, IGroupByResult> GroupByResultMap { get; set; }

        public GroupByFieldResult GetAsGroupByFieldResult(string groupByName)
        {
            if (GroupByResultMap == null || !GroupByResultMap.ContainsKey(groupByName))
            {
                throw new ArgumentException(string.Format("GroupByResults don't contains: {0}", groupByName));
            }

            IGroupByResult result = GroupByResultMap[groupByName];

            if (result.GetGroupByType() != GroupByType.GroupByField)
            {
                throw new ArgumentException(string.Format("the result with this groupByName[{0}] can't cast to GroupByFieldResult.", groupByName));
            }

            return (GroupByFieldResult)result;
        }

        public GroupByGeoDistanceResult GetAsGroupByGeoDistanceResult(string groupByName)
        {
            if (GroupByResultMap == null || !GroupByResultMap.ContainsKey(groupByName))
            {
                throw new ArgumentException(string.Format("GroupByResults don't contains: {0}", groupByName));
            }

            IGroupByResult result = GroupByResultMap[groupByName];

            if (result.GetGroupByType() != GroupByType.GroupByGeoDistance)
            {
                throw new ArgumentException(string.Format("the result with this groupByName[{0}] can't cast to GroupByGeoDistanceResult.", groupByName));
            }

            return (GroupByGeoDistanceResult)result;
        }

        public GroupByFilterResult GetAsGroupByFilterResult(string groupByName)
        {
            if (GroupByResultMap == null || !GroupByResultMap.ContainsKey(groupByName))
            {
                throw new ArgumentException(string.Format("GroupByResults don't contains: {0}", groupByName));
            }

            IGroupByResult result = GroupByResultMap[groupByName];

            if (result.GetGroupByType() != GroupByType.GroupByFilter)
            {
                throw new ArgumentException(string.Format("the result with this groupByName[{0}] can't cast to GroupByFilterResult.", groupByName));
            }

            return (GroupByFilterResult)result;
        }

        public GroupByRangeResult GetAsGroupByRangeResult(string groupByName)
        {
            if (GroupByResultMap == null || !GroupByResultMap.ContainsKey(groupByName))
            {
                throw new ArgumentException(string.Format("GroupByResults don't contains: {0}", groupByName));
            }

            IGroupByResult result = GroupByResultMap[groupByName];

            if (result.GetGroupByType() != GroupByType.GroupByRange)
            {
                throw new ArgumentException(string.Format("the result with this groupByName[{0}] can't cast to GroupByRangeResult.", groupByName));
            }

            return (GroupByRangeResult)result;
        }

        public GroupByHistogramResult GetAsGroupByHistogramResult(string groupByName)
        {
            if (GroupByResultMap == null || !GroupByResultMap.ContainsKey(groupByName))
            {
                throw new ArgumentException(string.Format("GroupByResults don't contains: {0}", groupByName));
            }

            IGroupByResult result = GroupByResultMap[groupByName];

            if (result.GetGroupByType() != GroupByType.GroupByHistogram)
            {
                throw new ArgumentException(string.Format("the result with this groupByName[{0}] can't cast to GroupByHistogramResult.", groupByName));
            }

            return (GroupByHistogramResult)result;
        }
    }
}
