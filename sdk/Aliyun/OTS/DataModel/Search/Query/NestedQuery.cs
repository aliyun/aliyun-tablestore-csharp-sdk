using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    ///嵌套查询可以查询嵌套的对象/文档。
    ///举例：我们的文档是这样的：{"id":"1","os":{"name":"win7","ip":"127.0.0.1"}}，我们想搜索os的name，
    ///但是不能直接查询，需要通过{@link NestedQuery}来进行查询。在"path"设置为“os”，然后query中放一个正常的Query
    /// </summary>
    public class NestedQuery : IQuery
    {
        /// <summary>
        /// 嵌套文档的路径
        /// </summary>
        public string Path { get; set; }
        /// <summary>
        /// 一个query
        /// </summary>
        public IQuery Query { get; set; }
        /// <summary>
        /// 多值字段获取文档得分的模式
        /// </summary>
        public ScoreMode ScoreMode { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_NestedQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildNestedQuery(this).ToByteString();
        }
    }
}
