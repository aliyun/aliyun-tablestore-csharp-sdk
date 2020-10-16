using System.Collections.Generic;
using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    public class TermsQuery : IQuery
    {
        public string FieldName { get; set; }

        public List<ColumnValue> Terms { get; set; } 

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_TermsQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildTermsQuery(this).ToByteString();
        }
    }
}
