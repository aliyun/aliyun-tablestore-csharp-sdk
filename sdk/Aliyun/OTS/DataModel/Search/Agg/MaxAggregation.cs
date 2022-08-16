using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 最大值统计
    /// </summary>
    public class MaxAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggMax;

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
            return SearchAggregationBuilder.BuildMaxAggregation(this).ToByteString();
        }
    }
}
