using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 和值统计
    /// </summary>
    public class SumAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggSum;

        public string AggName { get; set; }

        public string FieldName { get; set; }

        public ColumnValue Missing { get; set; }

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
            return SearchAggregationBuilder.BuildSumAggregation(this).ToByteString();
        }
    }
}
