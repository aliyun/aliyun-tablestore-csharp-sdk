using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 根据某一个字段统计去重后的文档总数。该总数为大约值，数据量特别大时存在一定的误差。
    /// </summary>
    public class DistinctCountAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggDistinctCount;

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
            return SearchAggregationBuilder.BuildDistinctCountAggregation(this).ToByteString();
        }
    }
}
