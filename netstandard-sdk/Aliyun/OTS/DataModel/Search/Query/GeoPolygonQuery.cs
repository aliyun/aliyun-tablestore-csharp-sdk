using System.Collections.Generic;
using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 找出落在指定多边形包围起来的图形内的数据
    ///注意：这个查询器使用代价很大，请避免使用</p>
    ///场景举例：小黄车只能在繁华的地方服务，出了市区要收额外的服务费，而繁华的城市的边界是多边形的。我们想查询该车辆是否需要付额外的服务费，就需要通过搜索用户的经纬度是否在多边形内。
    /// </summary>
    public class GeoPolygonQuery : IQuery
    {
        /// <summary>
        /// 字段名
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        ///  经纬度字符串的List
        /// </summary>
        public List<string> Points { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_GeoPolygonQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildGeoPolygonQuery(this).ToByteString();
        }
    }
}
