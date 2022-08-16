using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 最小值统计
    /// </summary>
    public class MinAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggMin;

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
            return SearchAggregationBuilder.BuildMinAggregation(this).ToByteString();
        }
    }
}
