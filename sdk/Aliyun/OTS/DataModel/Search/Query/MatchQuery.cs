using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 包括模糊匹配和短语或邻近查询
    /// </summary>
    public class MatchQuery : IQuery
    {
        /// <summary>
        /// 字段
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        /// 模糊匹配的值
        /// </summary>
        public string Text { get; set; }

        /// <summary>
        /// 最小匹配个数
        /// </summary>
        public int? MinimumShouldMatch { get; set; }

        /// <summary>
        /// 查询权重
        /// </summary>
        public float Weight { get; set; }

        /// <summary>
        /// 操作符
        /// </summary>
        public QueryOperator Operator { get; set; }

        public MatchQuery(string fieldName, string text)
        {
            FieldName = fieldName;
            Text = text;
            Weight = 1.0f;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_MatchQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildMatchQuery(this).ToByteString();
        }
    }
}
