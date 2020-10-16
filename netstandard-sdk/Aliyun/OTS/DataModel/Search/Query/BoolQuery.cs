using System.Collections.Generic;
using Google.ProtocolBuffers;
using com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    ///联合查询（复杂查询条件下用的最多的一个查询）。Bool查询对应Lucene中的BooleanQuery，它由一个或者多个子句组成，每个子句都有特定的类型。
    ///must: 文档必须完全匹配条件
    ///should: should下面会带一个以上的条件，至少满足一个条件，这个文档就符合should
    ///must_not: 文档必须不匹配条件
    /// </summary>
    public class BoolQuery : IQuery
    {
        /// <summary>
        /// 文档必须完全匹配所有的子query
        /// </summary>
        public List<IQuery> MustQueries { get; set; }
        /// <summary>
        /// 文档必须不能匹配任何子query
        /// </summary>
        public List<IQuery> MustNotQueries { get; set; }
        /// <summary>
        /// 文档必须完全匹配所有的子filter，filter类似于query，区别是不会进行算分
        /// </summary>
        public List<IQuery> FilterQueries { get; set; }
        /// <summary>
        /// 文档应该至少匹配一个should，匹配多的得分会高
        /// </summary>
        public List<IQuery> ShouldQueries { get; set; }
        /// <summary>
        /// 定义了至少满足几个should子句，默认是1。
        /// </summary>
        public int? MinimumShouldMatch { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_BoolQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildBoolQuery(this).ToByteString();
        }
    }
}
