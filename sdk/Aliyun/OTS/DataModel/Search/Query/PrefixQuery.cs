using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 匹配前缀。比如搜索“name”是以“王”字开头的所有人。
    /// </summary>
    public class PrefixQuery : IQuery
    {
        public string FieldName { get; set; }
        /**
         * 	字符串前缀
         */
        public string Prefix { get; set; }

        public float Weight { get; set; }

        public PrefixQuery(string fieldName, string prefix)
        {
            FieldName = fieldName;
            Prefix = prefix;
            Weight = 1.0f;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_PrefixQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildPrefixQuery(this).ToByteString();
        }
    }
}
