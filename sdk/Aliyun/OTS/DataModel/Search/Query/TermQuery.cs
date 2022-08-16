using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 精确的term查询。
    /// </summary>
    public class TermQuery : IQuery
    {
        public string FieldName { get; set; }
        public ColumnValue Term { get; set; }
        public float Weight { get; set; }

        public TermQuery(string fieldName, ColumnValue term)
        {
            FieldName = fieldName;
            Term = term;
            Weight = 1.0f;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_TermQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildTermQuery(this).ToByteString();
        }
    }
}
