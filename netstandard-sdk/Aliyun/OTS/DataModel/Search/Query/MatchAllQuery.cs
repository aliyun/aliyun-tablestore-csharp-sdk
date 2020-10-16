using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 获取所有的文档，所有文档分数为1。返回的结果中：命中数永远都是正确的。加入返回的结果过多，SearchIndex会只返回部分数据。
    /// </summary>
    public class MatchAllQuery : IQuery
    {
        public QueryType GetQueryType()
        {
            return QueryType.QueryType_MatchAllQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildMatchAllQuery().ToByteString();
        }
    }
}
