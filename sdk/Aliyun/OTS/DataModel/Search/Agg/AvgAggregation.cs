using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class AvgAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggAvg;
        /// <summary>
        /// 聚合的名称，之后从聚合结果列表中根据该名字获取到聚合结果
        /// </summary>
        public string AggName { get; set; }
        /// <summary>
        /// 字段名称
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        /// 确实字段的默认值。
        /// 如果一个文档缺少该字段，则采用什么默认值。
        /// </summary>
        public ColumnValue Missing;

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
            return SearchAggregationBuilder.BuildAvgAggregation(this).ToByteString();
        }
    }
}
