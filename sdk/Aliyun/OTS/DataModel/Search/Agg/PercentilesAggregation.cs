using System.Collections.Generic;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    /// <summary>
    /// 百分位统计。统计不同百分位下的数据行数
    /// </summary>
    public class PercentilesAggregation : IAggregation
    {
        private readonly AggregationType AggregationType = AggregationType.AggPercentiles;

        public string AggName { get; set; }

        public string FieldName { get; set; }
        /// <summary>
        /// 百分位范围。
        /// <b>必填，例如：[0, 90, 99]</b>
        /// </summary>
        public List<double> Percentiles { get; set; }

        public ColumnValue Missing { get; set; }

        public string GetAggName()
        {
            return AggName;
        }

        public AggregationType GetAggType()
        {
            return this.AggregationType;
        }

        public ByteString Serialize()
        {
            return SearchAggregationBuilder.BuildPercentilesAggregation(this).ToByteString();
        }
    }
}
