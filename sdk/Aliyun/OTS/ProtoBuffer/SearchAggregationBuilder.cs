using Aliyun.OTS.DataModel.Search.Agg;
using Google.ProtocolBuffers;
using System;
using System.Collections.Generic;
using PB = com.alicloud.openservices.tablestore.core.protocol;
namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchAggregationBuilder
    {
        private static PB.AggregationType BuildAggregationType(AggregationType type)
        {
            switch (type)
            {
                case AggregationType.AggAvg:
                    return PB.AggregationType.AGG_AVG;
                case AggregationType.AggMin:
                    return PB.AggregationType.AGG_MIN;
                case AggregationType.AggMax:
                    return PB.AggregationType.AGG_MAX;
                case AggregationType.AggSum:
                    return PB.AggregationType.AGG_SUM;
                case AggregationType.AggCount:
                    return PB.AggregationType.AGG_COUNT;
                case AggregationType.AggDistinctCount:
                    return PB.AggregationType.AGG_DISTINCT_COUNT;
                case AggregationType.AggTopRows:
                    return PB.AggregationType.AGG_TOP_ROWS;
                case AggregationType.AggPercentiles:
                    return PB.AggregationType.AGG_PERCENTILES;
                default:
                    throw new ArgumentException("unknown AggregationType: {0}", type.ToString());
            }
        }

        public static PB.MaxAggregation BuildMaxAggregation(MaxAggregation agg)
        {
            PB.MaxAggregation.Builder builder = PB.MaxAggregation.CreateBuilder();

            builder.SetFieldName(agg.FieldName);
            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }

            return builder.Build();
        }

        public static PB.AvgAggregation BuildAvgAggregation(AvgAggregation agg)
        {
            PB.AvgAggregation.Builder builder = PB.AvgAggregation.CreateBuilder();

            builder.SetFieldName(agg.FieldName);
            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }

            return builder.Build();
        }

        public static PB.MinAggregation BuildMinAggregation(MinAggregation agg)
        {
            PB.MinAggregation.Builder builder = PB.MinAggregation.CreateBuilder();
            builder.SetFieldName(agg.FieldName);
            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }
            return builder.Build();
        }

        public static PB.SumAggregation BuildSumAggregation(SumAggregation agg)
        {
            PB.SumAggregation.Builder builder = PB.SumAggregation.CreateBuilder();
            builder.SetFieldName(agg.FieldName);
            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }
            return builder.Build();
        }

        public static PB.CountAggregation BuildCountAggregation(CountAggregation agg)
        {
            PB.CountAggregation.Builder builder = new PB.CountAggregation.Builder();
            builder.SetFieldName(agg.FieldName);
            return builder.Build();
        }

        public static PB.DistinctCountAggregation BuildDistinctCountAggregation(DistinctCountAggregation agg)
        {
            PB.DistinctCountAggregation.Builder builder = PB.DistinctCountAggregation.CreateBuilder();
            builder.SetFieldName(agg.FieldName);
            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }
            return builder.Build();
        }

        public static PB.TopRowsAggregation BuildTopRowsAggregation(TopRowsAggregation agg)
        {
            PB.TopRowsAggregation.Builder builder = PB.TopRowsAggregation.CreateBuilder();

            if (agg.Limit.HasValue)
            {
                builder.SetLimit(agg.Limit.Value);
            }

            if (agg.Sort != null)
            {
                builder.SetSort(SearchSortBuilder.BuildSort(agg.Sort));
            }

            return builder.Build();
        }

        public static PB.PercentilesAggregation BuildPercentilesAggregation(PercentilesAggregation agg)
        {
            PB.PercentilesAggregation.Builder builder = PB.PercentilesAggregation.CreateBuilder();

            builder.SetFieldName(agg.FieldName);

            if (agg.Percentiles != null)
            {
                builder.AddRangePercentiles(agg.Percentiles);
            }

            if (agg.Missing != null)
            {
                builder.SetMissing(ByteString.CopyFrom(SearchVariantType.ToVariant(agg.Missing)));
            }

            return builder.Build();
        }

        public static PB.Aggregation BuildAggregation(IAggregation aggregation)
        {
            PB.Aggregation.Builder builder = PB.Aggregation.CreateBuilder();
            builder.SetName(aggregation.GetAggName());
            builder.SetType(BuildAggregationType(aggregation.GetAggType()));
            builder.SetBody(aggregation.Serialize());
            return builder.Build();
        }

        public static PB.Aggregations BuildAggregations(List<IAggregation> aggregations)
        {
            PB.Aggregations.Builder builder = PB.Aggregations.CreateBuilder();
            foreach (IAggregation IAgg in aggregations)
            {
                builder.AddAggs(BuildAggregation(IAgg));
            }
            return builder.Build();
        }
    }
}
