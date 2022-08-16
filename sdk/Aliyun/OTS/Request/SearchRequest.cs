using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Request
{
    public class SearchRequest : OTSRequest
    {
        /// <summary>
        ///  TableStore的表名
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        ///  SearchIndex中的index名
        /// </summary>
        public string IndexName { get; set; }
        /// <summary>
        /// 查询语句，具体参数详见{@link SearchQuery}
        /// </summary>
        public SearchQuery SearchQuery { get; set; }
        /// <summary>
        /// 指定哪些属性列需要返回
        ///如果SearchIndex中的属性列太多，而只想要某些属性列，则可以减少网络传输的数据量，提高响应速度
        /// </summary>
        public ColumnsToGet ColumnsToGet { get; set; }
        /// <summary>
        /// 路由字段
        ///默认为空，大多数场景下不需要使用该值。如果使用了自定义路由，可以指定路由字段。
        ///注意：<b>高级特性</b>。如需了解或使用请提工单或联系开发人员
        /// </summary>
        public List<PrimaryKey> RoutingValues { get; set; }

        /// <summary>
        /// 超时字段
        /// 请求级别的超时字段，单位ms。默认为-1。
        /// </summary>
        public int TimeoutInMillisecond { get; set; }

        public SearchRequest(string tableName, string indexName, SearchQuery searchQuery)
        {
            TableName = tableName;
            IndexName = indexName;
            SearchQuery = searchQuery;
            TimeoutInMillisecond = -1;
        }
    }
}
