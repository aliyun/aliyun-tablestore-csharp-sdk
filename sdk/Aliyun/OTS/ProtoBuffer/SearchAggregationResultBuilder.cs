using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search.Agg;
using Google.ProtocolBuffers;
using System.Collections.Generic;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Aliyun.OTS.ProtoBuffer;
using System.IO;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchAggregationResultBuilder
    {
        private static AvgAggregationResult BuildAvgAggregationResult(string aggName, ByteString aggBody)
        {
            PB.AvgAggregationResult aggResult = PB.AvgAggregationResult.ParseFrom(aggBody);

            AvgAggregationResult result = new AvgAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static MaxAggregationResult BuildMaxAggregationResult(string aggName, ByteString aggBody)
        {
            PB.MaxAggregationResult aggResult = PB.MaxAggregationResult.ParseFrom(aggBody);

            MaxAggregationResult result = new MaxAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static MinAggregationResult BuildMinAggregationResult(string aggName, ByteString aggBody)
        {

            PB.MinAggregationResult aggResult = PB.MinAggregationResult.ParseFrom(aggBody);

            MinAggregationResult result = new MinAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static SumAggregationResult BuildSumAggregationResult(string aggName, ByteString aggBody)
        {
            PB.SumAggregationResult aggResult = PB.SumAggregationResult.ParseFrom(aggBody);

            SumAggregationResult result = new SumAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static DistinctCountAggregationResult BuildDistinctCountAggregationResult(string aggName, ByteString aggBody)
        {
            PB.DistinctCountAggregationResult aggResult = PB.DistinctCountAggregationResult.ParseFrom(aggBody);

            DistinctCountAggregationResult result = new DistinctCountAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static CountAggregationResult BuildCountAggregationResult(string aggName, ByteString aggBody)
        {
            PB.CountAggregationResult aggResult = PB.CountAggregationResult.ParseFrom(aggBody);

            CountAggregationResult result = new CountAggregationResult
            {
                AggName = aggName,
                Value = aggResult.Value
            };

            return result;
        }

        private static TopRowsAggregationResult BuildTopRowsAggregationResult(string aggName, ByteString aggBody)
        {
            PB.TopRowsAggregationResult aggResult = PB.TopRowsAggregationResult.ParseFrom(aggBody);

            TopRowsAggregationResult result = new TopRowsAggregationResult
            {
                AggName = aggName,
            };

            List<Row> rows = new List<Row>(aggResult.RowsCount);
            foreach (ByteString byteString in aggResult.RowsList)
            {
                rows.Add(BuildRow(byteString));
            }

            result.Rows = rows;

            return result;
        }

        private static PercentilesAggregationResult BuildPercentilesAggregationResult(string aggName, ByteString aggBody)
        {
            PB.PercentilesAggregationResult aggResult = PB.PercentilesAggregationResult.ParseFrom(aggBody);

            PercentilesAggregationResult result = new PercentilesAggregationResult
            {
                AggName = aggName
            };

            if (aggResult.PercentilesAggregationItemsCount == 0)
            {
                return result;
            }

            List<PercentilesAggregationResultItem> percentilesAggregationResultItems = new List<PercentilesAggregationResultItem>(aggResult.PercentilesAggregationItemsCount);
            foreach (PB.PercentilesAggregationItem item in aggResult.PercentilesAggregationItemsList)
            {
                percentilesAggregationResultItems.Add(BuildPercentilesAggregationItem(item));
            }

            result.Value = percentilesAggregationResultItems;

            return result;
        }

        private static DataModel.Row BuildRow(ByteString byteString)
        {
            PB.PlainBufferCodedInputStream inputStream = new PB.PlainBufferCodedInputStream(byteString.CreateCodedInput());
            List<PB.PlainBufferRow> rows = inputStream.ReadRowsWithHeader();
            if (rows.Count != 1)
            {
                throw new IOException("Expect only returns one row. Row count: " + rows.Count);
            }

            return PB.PlainBufferConversion.ToRow(rows[0]) as DataModel.Row;
        }

        private static IAggregationResult BuildAggregationResult(PB.AggregationResult aggregationResult)
        {
            if (!aggregationResult.HasType)
            {
                throw new OTSClientException("no aggregation type info in result.");
            }

            switch (aggregationResult.Type)
            {
                case PB.AggregationType.AGG_AVG:
                    return BuildAvgAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_COUNT:
                    return BuildCountAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_DISTINCT_COUNT:
                    return BuildDistinctCountAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_MAX:
                    return BuildMaxAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_MIN:
                    return BuildMinAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_PERCENTILES:
                    return BuildPercentilesAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_SUM:
                    return BuildSumAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                case PB.AggregationType.AGG_TOP_ROWS:
                    return BuildTopRowsAggregationResult(aggregationResult.Name, aggregationResult.AggResult);
                default:
                    throw new OTSClientException(string.Format("unsupported aggType: {0}", aggregationResult.Type.ToString()));
            }
        }

        public static AggregationResults BuildAggregationresults(PB.AggregationsResult aggregationsResult)
        {
            AggregationResults aggregationResults = new AggregationResults();
            Dictionary<string, IAggregationResult> aggregationResultMap = new Dictionary<string, IAggregationResult>();

            if (aggregationsResult.AggResultsCount == 0)
            {
                return aggregationResults;
            }

            foreach (PB.AggregationResult result in aggregationsResult.AggResultsList)
            {
                aggregationResultMap.Add(result.Name, BuildAggregationResult(result));
            }

            aggregationResults.ResultMap = aggregationResultMap;

            return aggregationResults;
        }

        public static AggregationResults BuildAggregationResultsFromByteString(ByteString agg)
        {
            PB.AggregationsResult aggregationsResult = PB.AggregationsResult.ParseFrom(agg);

            return BuildAggregationresults(aggregationsResult);
        }

        private static PercentilesAggregationResultItem BuildPercentilesAggregationItem(PB.PercentilesAggregationItem item)
        {
            PercentilesAggregationResultItem result = new PercentilesAggregationResultItem();
            if (item.HasKey)
            {
                result.Key = item.Key;
            }

            if (item.HasValue)
            {
                result.Value = SearchVariantType.ForceConvertToDestColumnValue(item.Value.ToByteArray());
            }

            return result;
        }
    }
}
