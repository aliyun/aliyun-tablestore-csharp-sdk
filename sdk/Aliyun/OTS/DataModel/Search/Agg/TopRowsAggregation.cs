using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class TopRowsAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggTopRows;

        public string AggName { get; set; }

        public int? Limit { get; set; }

        public Sort.Sort Sort { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return AggregationType;
        }

        public ByteString Serialize()
        {
            return SearchAggregationBuilder.BuildTopRowsAggregation(this).ToByteString();
        }

    }
}
