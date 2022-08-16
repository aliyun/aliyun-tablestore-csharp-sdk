using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    public class ExistsQuery : IQuery
    {
        public string FieldName { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_ExistsQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildExistQuery(this).ToByteString();
        }
    }
}
