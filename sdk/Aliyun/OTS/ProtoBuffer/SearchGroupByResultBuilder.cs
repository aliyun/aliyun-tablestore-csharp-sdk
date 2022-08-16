using System.Collections.Generic;
using Aliyun.OTS.DataModel.Search.GroupBy;
using Google.ProtocolBuffers;
using PB = com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchGroupByResultBuilder
    {
        private static IGroupByResult BuildGroupByFieldResult(string groupByName, ByteString groupByBody)
        {
            PB.GroupByFieldResult groupByResult = PB.GroupByFieldResult.ParseFrom(groupByBody);

            GroupByFieldResult result = new GroupByFieldResult
            {
                GroupByName = groupByName
            };

            if (groupByResult.GroupByFieldResultItemsList == null || groupByResult.GroupByFieldResultItemsCount == 0)
            {
                return result;
            }

            List<GroupByFieldResultItem> items = new List<GroupByFieldResultItem>();

            foreach (PB.GroupByFieldResultItem item in groupByResult.GroupByFieldResultItemsList)
            {
                items.Add(BuildGroupByFieldResultItem(item));
            }

            result.GroupByFieldResultItems = items;

            return result;
        }

        private static GroupByFieldResultItem BuildGroupByFieldResultItem(PB.GroupByFieldResultItem item)
        {
            GroupByFieldResultItem result = new GroupByFieldResultItem();

            if (item.HasKey)
            {
                result.Key = item.Key;
            }

            if (item.HasRowCount)
            {
                result.RowCount = item.RowCount;
            }

            if (item.HasSubAggsResult)
            {
                result.SubAggregationResults = SearchAggregationResultBuilder.BuildAggregationresults(item.SubAggsResult);
            }

            if (item.HasSubGroupBysResult)
            {
                result.SubGroupByResults = BuildGroupByResults(item.SubGroupBysResult);
            }

            return result;
        }

        private static GroupByRangeResultItem BuildGroupByRangeResultItem(PB.GroupByRangeResultItem item)
        {
            GroupByRangeResultItem result = new GroupByRangeResultItem();

            if (item.HasFrom)
            {
                result.From = item.From;
            }

            if (item.HasTo)
            {
                result.To = item.To;
            }

            if (item.HasRowCount)
            {
                result.RowCount = item.RowCount;
            }

            if (item.HasSubAggsResult)
            {
                result.SubAggregationResults = SearchAggregationResultBuilder.BuildAggregationresults(item.SubAggsResult);
            }

            if (item.HasSubGroupBysResult)
            {
                result.SubGroupByResults = BuildGroupByResults(item.SubGroupBysResult);
            }

            return result;
        }

        private static GroupByRangeResult BuildGroupByRangeResult(string groupByName, ByteString groupByBody)
        {
            PB.GroupByRangeResult groupByResult = PB.GroupByRangeResult.ParseFrom(groupByBody);

            GroupByRangeResult result = new GroupByRangeResult
            {
                GroupByName = groupByName
            };

            if (groupByResult.GroupByRangeResultItemsList == null || groupByResult.GroupByRangeResultItemsCount == 0)
            {
                return result;
            }

            List<GroupByRangeResultItem> items = new List<GroupByRangeResultItem>();

            foreach (PB.GroupByRangeResultItem item in groupByResult.GroupByRangeResultItemsList)
            {
                items.Add(BuildGroupByRangeResultItem(item));
            }

            result.GroupByRangeResultItems = items;

            return result;
        }

        private static GroupByHistogramResultItem BuildGroupByHistogramResultItem(PB.GroupByHistogramItem item)
        {
            GroupByHistogramResultItem result = new GroupByHistogramResultItem();

            if (item.HasKey)
            {
                result.Key = SearchVariantType.ForceConvertToDestColumnValue(item.Key.ToByteArray());
            }

            if (item.HasValue)
            {
                result.Value = item.Value;
            }

            if (item.HasSubAggsResult)
            {
                result.SubAggregationResults = SearchAggregationResultBuilder.BuildAggregationresults(item.SubAggsResult);
            }

            if (item.HasSubGroupBysResult)
            {
                result.SubGroupByResults = BuildGroupByResults(item.SubGroupBysResult);
            }

            return result;
        }

        private static GroupByHistogramResult BuildGroupByHistogramResult(string groupByName, ByteString groupByBody)
        {
            PB.GroupByHistogramResult groupByResult = PB.GroupByHistogramResult.ParseFrom(groupByBody);

            GroupByHistogramResult result = new GroupByHistogramResult
            {
                GroupByName = groupByName
            };

            if (groupByResult.GroupByHistograItemsList == null || groupByResult.GroupByHistograItemsCount == 0)
            {
                return result;
            }

            List<GroupByHistogramResultItem> items = new List<GroupByHistogramResultItem>();

            foreach (PB.GroupByHistogramItem item in groupByResult.GroupByHistograItemsList)
            {
                items.Add(BuildGroupByHistogramResultItem(item));
            }

            result.GroupByHistogramResultItems = items;

            return result;
        }

        private static GroupByGeoDistanceResultItem BuildGroupByGeoDistanceResultItem(PB.GroupByGeoDistanceResultItem item)
        {
            GroupByGeoDistanceResultItem result = new GroupByGeoDistanceResultItem();

            if (item.HasFrom)
            {
                result.From = item.From;
            }

            if (item.HasTo)
            {
                result.To = item.To;
            }

            if (item.HasRowCount)
            {
                result.RowCount = item.RowCount;
            }

            if (item.HasSubAggsResult)
            {
                result.SubAggregationResults = SearchAggregationResultBuilder.BuildAggregationresults(item.SubAggsResult);
            }

            if (item.HasSubGroupBysResult)
            {
                result.SubGroupByResults = BuildGroupByResults(item.SubGroupBysResult);
            }

            return result;
        }

        private static GroupByGeoDistanceResult BuildGroupByGeoDistanceResult(string groupByName, ByteString groupByBody)
        {
            PB.GroupByGeoDistanceResult groupByResult = PB.GroupByGeoDistanceResult.ParseFrom(groupByBody);

            GroupByGeoDistanceResult result = new GroupByGeoDistanceResult
            {
                GroupByName = groupByName
            };

            if (groupByResult.GroupByGeoDistanceResultItemsList == null || groupByResult.GroupByGeoDistanceResultItemsCount == 0)
            {
                return result;
            }

            List<GroupByGeoDistanceResultItem> items = new List<GroupByGeoDistanceResultItem>();

            foreach (PB.GroupByGeoDistanceResultItem item in groupByResult.GroupByGeoDistanceResultItemsList)
            {
                items.Add(BuildGroupByGeoDistanceResultItem(item));
            }

            result.GroupByGeoDistanceResultItems = items;

            return result;
        }

        private static GroupByFilterResultItem BuildGroupByFilterResultItem(PB.GroupByFilterResultItem item)
        {
            GroupByFilterResultItem result = new GroupByFilterResultItem();

            if (item.HasRowCount)
            {
                result.RowCount = item.RowCount;
            }

            if (item.HasSubAggsResult)
            {
                result.SubAggregationResults = SearchAggregationResultBuilder.BuildAggregationresults(item.SubAggsResult);
            }

            if (item.HasSubGroupBysResult)
            {
                result.SubGroupByResults = BuildGroupByResults(item.SubGroupBysResult);
            }

            return result;
        }

        private static GroupByFilterResult BuildGroupByFilterResult(string groupByName, ByteString groupByBody)
        {
            PB.GroupByFilterResult groupByResult = PB.GroupByFilterResult.ParseFrom(groupByBody);

            GroupByFilterResult result = new GroupByFilterResult
            {
                GroupByName = groupByName
            };

            if (groupByResult.GroupByFilterResultItemsList == null || groupByResult.GroupByFilterResultItemsCount == 0)
            {
                return result;
            }

            List<GroupByFilterResultItem> items = new List<GroupByFilterResultItem>();

            foreach (PB.GroupByFilterResultItem item in groupByResult.GroupByFilterResultItemsList)
            {
                items.Add(BuildGroupByFilterResultItem(item));
            }
            result.GroupByFilterResultItems = items;

            return result;
        }

        private static IGroupByResult BuildGroupByResult(PB.GroupByResult groupByResult)
        {
            if (!groupByResult.HasType)
            {
                return null;
            }

            switch (groupByResult.Type)
            {
                case PB.GroupByType.GROUP_BY_FIELD:
                    return BuildGroupByFieldResult(groupByResult.Name, groupByResult.GroupByResult_);
                case PB.GroupByType.GROUP_BY_FILTER:
                    return BuildGroupByFilterResult(groupByResult.Name, groupByResult.GroupByResult_);
                case PB.GroupByType.GROUP_BY_GEO_DISTANCE:
                    return BuildGroupByGeoDistanceResult(groupByResult.Name, groupByResult.GroupByResult_);
                case PB.GroupByType.GROUP_BY_HISTOGRAM:
                    return BuildGroupByHistogramResult(groupByResult.Name, groupByResult.GroupByResult_);
                case PB.GroupByType.GROUP_BY_RANGE:
                    return BuildGroupByRangeResult(groupByResult.Name, groupByResult.GroupByResult_);
                default:
                    throw new OTSClientException(string.Format("unsupported groupBy type: {0}", groupByResult.Type));
            }
        }

        private static GroupByResults BuildGroupByResults(PB.GroupBysResult groupBysResult)
        {
            GroupByResults groupByResults = new GroupByResults();

            Dictionary<string, IGroupByResult> groupByResultMap = new Dictionary<string, IGroupByResult>();

            if (groupBysResult.GroupByResultsList == null || groupBysResult.GroupByResultsCount == 0)
            {
                return groupByResults;
            }

            foreach (PB.GroupByResult groupByResult in groupBysResult.GroupByResultsList)
            {
                groupByResultMap.Add(groupByResult.Name, BuildGroupByResult(groupByResult));
            }

            groupByResults.GroupByResultMap = groupByResultMap;

            return groupByResults;
        }

        public static GroupByResults BuildGroupByResultsFromByteString(ByteString groupBy)
        {
            PB.GroupBysResult aggregationsResult = PB.GroupBysResult.ParseFrom(groupBy);

            return BuildGroupByResults(aggregationsResult);
        }
    }
}
