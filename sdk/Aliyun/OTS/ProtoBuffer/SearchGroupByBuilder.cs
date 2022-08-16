using Aliyun.OTS.DataModel.Search.GroupBy;
using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;
using System;
using System.Collections.Generic;
using PB = com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchGroupByBuilder
    {
        private static PB.GroupByType BuildGroupByType(DataModel.Search.GroupBy.GroupByType type)
        {
            switch (type)
            {
                case DataModel.Search.GroupBy.GroupByType.GroupByField:
                    return PB.GroupByType.GROUP_BY_FIELD;
                case DataModel.Search.GroupBy.GroupByType.GroupByRange:
                    return PB.GroupByType.GROUP_BY_RANGE;
                case DataModel.Search.GroupBy.GroupByType.GroupByFilter:
                    return PB.GroupByType.GROUP_BY_FILTER;
                case DataModel.Search.GroupBy.GroupByType.GroupByGeoDistance:
                    return PB.GroupByType.GROUP_BY_GEO_DISTANCE;
                case DataModel.Search.GroupBy.GroupByType.GroupByHistogram:
                    return PB.GroupByType.GROUP_BY_HISTOGRAM;
                default:
                    throw new ArgumentException(string.Format("unknown GroupByType: {0}", type.ToString()));
            }
        }

        public static PB.GroupByField BuildGroupByField(DataModel.Search.GroupBy.GroupByField groupBy)
        {
            PB.GroupByField.Builder builder = PB.GroupByField.CreateBuilder();
            builder.SetFieldName(groupBy.FieldName);

            if (groupBy.Size.HasValue)
            {
                builder.SetSize(groupBy.Size.Value);
            }

            if (groupBy.MinDocCount.HasValue)
            {
                builder.SetMinDocCount(groupBy.MinDocCount.Value);
            }

            if (groupBy.SubAggregations != null)
            {
                builder.SetSubAggs(SearchAggregationBuilder.BuildAggregations(groupBy.SubAggregations));
            }

            if (groupBy.SubGroupBys != null)
            {
                builder.SetSubGroupBys(BuildGroupBys(groupBy.SubGroupBys));
            }

            if (groupBy.GroupBySorters != null)
            {
                builder.SetSort(SearchSortBuilder.BuildGroupBySort(groupBy.GroupBySorters));
            }

            return builder.Build();
        }

        public static PB.GroupByHistogram BuildGroupByHistogram(DataModel.Search.GroupBy.GroupByHistogram groupBy)
        {
            PB.GroupByHistogram.Builder builder = PB.GroupByHistogram.CreateBuilder();

            if (groupBy.FieldName != null)
            {
                builder.SetFieldName(groupBy.FieldName);
            }

            if (groupBy.Interval != null)
            {
                builder.SetInterval(ByteString.CopyFrom(SearchVariantType.ToVariant(groupBy.Interval)));
            }

            if (groupBy.MinDocCount.HasValue)
            {
                builder.SetMinDocCount(groupBy.MinDocCount.Value);
            }

            if (groupBy.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(groupBy.Missing)));
            }

            if (groupBy.GroupBySorters != null)
            {
                builder.SetSort(SearchSortBuilder.BuildGroupBySort(groupBy.GroupBySorters));
            }

            if (groupBy.SubAggregations != null)
            {
                builder.SetSubAggs(SearchAggregationBuilder.BuildAggregations(groupBy.SubAggregations));
            }

            if (groupBy.SubGroupBys != null)
            {
                builder.SetSubGroupBys(SearchGroupByBuilder.BuildGroupBys(groupBy.SubGroupBys));
            }

            if (groupBy.FieldRange != null)
            {
                builder.SetFieldRange(BuildFieldRange(groupBy.FieldRange));
            }

            return builder.Build();
        }

        public static PB.GroupByGeoDistance BuildGroupByGeoDistance(DataModel.Search.GroupBy.GroupByGeoDistance groupBy)
        {
            PB.GroupByGeoDistance.Builder builder = PB.GroupByGeoDistance.CreateBuilder();
            builder.SetFieldName(groupBy.FieldName);

            if (groupBy.Origin == null)
            {
                throw new ArgumentException("GroupByGeoDistance must set origin.");
            }

            builder.SetOrigin(BuildGeoPoint(groupBy.Origin));

            if (groupBy.Ranges == null || groupBy.Ranges.Count == 0)
            {
                throw new ArgumentException("GroupByGeoDistance must add range.");
            }
            foreach (DataModel.Search.GroupBy.Range range in groupBy.Ranges)
            {
                builder.AddRanges(BuildRange(range));
            }
            if (groupBy.SubGroupBys != null)
            {
                builder.SetSubGroupBys(BuildGroupBys(groupBy.SubGroupBys));
            }
            if (groupBy.SubAggregations != null)
            {
                builder.SetSubAggs(SearchAggregationBuilder.BuildAggregations(groupBy.SubAggregations));
            }
            return builder.Build();
        }

        public static PB.GroupByFilter BuildGroupByFilter(DataModel.Search.GroupBy.GroupByFilter groupBy)
        {
            PB.GroupByFilter.Builder builder = PB.GroupByFilter.CreateBuilder();

            if (groupBy.Filters != null)
            {
                foreach (DataModel.Search.Query.IQuery filter in groupBy.Filters)
                {
                    builder.AddFilters(SearchQueryBuilder.BuildQuery(filter));
                }
            }

            if (groupBy.SubGroupBys != null)
            {
                builder.SetSubGroupBys(BuildGroupBys(groupBy.SubGroupBys));
            }

            if (groupBy.SubAggregations != null)
            {
                builder.SetSubAggs(SearchAggregationBuilder.BuildAggregations(groupBy.SubAggregations));
            }

            return builder.Build();
        }

        public static PB.GroupByRange BuildGroupByRange(DataModel.Search.GroupBy.GroupByRange groupBy)
        {
            PB.GroupByRange.Builder builder = PB.GroupByRange.CreateBuilder();

            builder.SetFieldName(groupBy.FieldName);

            if (groupBy.Ranges == null || groupBy.Ranges.Count == 0)
            {
                throw new ArgumentException("GroupByGeoDistance must add range.");
            }
            foreach (DataModel.Search.GroupBy.Range range in groupBy.Ranges)
            {
                builder.AddRanges(BuildRange(range));
            }

            if (groupBy.SubGroupBys != null)
            {
                builder.SetSubGroupBys(BuildGroupBys(groupBy.SubGroupBys));
            }

            if (groupBy.SubAggregations != null)
            {
                builder.SetSubAggs(SearchAggregationBuilder.BuildAggregations(groupBy.SubAggregations));
            }

            return builder.Build();
        }

        private static PB.Range BuildRange(DataModel.Search.GroupBy.Range range)
        {
            PB.Range.Builder builder = PB.Range.CreateBuilder();

            if (range.From.HasValue && !range.From.Value.Equals(double.MinValue)) 
            { 
                builder.SetFrom(range.From.Value);
            }

            if (range.To.HasValue && !range.To.Value.Equals(double.MaxValue))
            {
                builder.SetTo(range.To.Value);
            }

            return builder.Build();
        }

        private static PB.GeoPoint BuildGeoPoint(DataModel.Search.GeoPoint geoPoint)
        {
            PB.GeoPoint.Builder builder = PB.GeoPoint.CreateBuilder();
            builder.SetLat(geoPoint.Lat);
            builder.SetLon(geoPoint.Lon);
            return builder.Build();
        }

        private static PB.GroupBy BuildGroupBy(IGroupBy groupBy)
        {
            PB.GroupBy.Builder builder = PB.GroupBy.CreateBuilder();
            builder.SetName(groupBy.GetGroupByName());
            builder.SetType(BuildGroupByType(groupBy.GetGroupByType()));
            builder.SetBody(groupBy.Serialize());
            return builder.Build();

        }

        public static PB.GroupBys BuildGroupBys(List<IGroupBy> groupBys)
        {
            PB.GroupBys.Builder builder = PB.GroupBys.CreateBuilder();
            foreach (IGroupBy groupBy in groupBys)
            {
                builder.AddGroupBys_(BuildGroupBy(groupBy));
            }
            return builder.Build();
        }

        private static PB.FieldRange BuildFieldRange(DataModel.Search.GroupBy.FieldRange groupBy)
        {
            PB.FieldRange.Builder builder = PB.FieldRange.CreateBuilder();
            if (groupBy.Max != null)
            {
                builder.SetMax(ByteString.CopyFrom(SearchVariantType.ToVariant(groupBy.Max)));
            }

            if (groupBy.Min != null)
            {
                builder.SetMin(ByteString.CopyFrom(SearchVariantType.ToVariant(groupBy.Min)));
            }

            return builder.Build();
        }
    }
}
