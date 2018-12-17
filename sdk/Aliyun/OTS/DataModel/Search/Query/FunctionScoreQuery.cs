using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 用于处理文档分值的Query，它会在查询结束后对每一个匹配的文档进行一系列的重打分操作，最后以生成的最终分数进行排序。
    ///举例见{@link FieldValueFactor}
    /// </summary>
    public class FunctionScoreQuery : IQuery
    {
        public IQuery Query { get; set; }
        public FieldValueFactor FieldValueFactor { get; set; }

        public FunctionScoreQuery(IQuery query, FieldValueFactor fieldValueFactor)
        {
            this.Query = query;
            this.FieldValueFactor = fieldValueFactor;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_FunctionScoreQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildFunctionScoreQuery(this).ToByteString();
        }
    }
}
