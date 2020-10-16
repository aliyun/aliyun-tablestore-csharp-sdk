using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 找出与某个位置某个距离内的数据。
    ///常用场景：搜索我附近1千米内的人。</p>
    ///通过设置我的centerPoint（一个经纬度信息），然后设置举例信息distanceInMeter=1000，进行查询即可
    /// </summary>
    public class GeoDistanceQuery : IQuery
    {
        /// <summary>
        /// 字段名
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        /// 中心点
        /// </summary>
        public string CenterPoint { get; set; }
        /// <summary>
        /// 与中心点的距离（单位：米）
        /// </summary>
        public double DistanceInMeter { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_GeoDistanceQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildGeoDistanceQuery(this).ToByteString();
        }
    }
}
