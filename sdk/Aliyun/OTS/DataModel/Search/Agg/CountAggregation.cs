using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 根据某一个字段统计文档数
    /// </summary>
    public class CountAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggCount;

        public string AggName { get; set; }

        public string FieldName { get; set; }

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
            return SearchAggregationBuilder.BuildCountAggregation(this).ToByteString();
        }
    }
}
