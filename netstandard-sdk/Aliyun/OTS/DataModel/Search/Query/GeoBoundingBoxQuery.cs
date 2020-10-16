using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 找出经纬度落在指定矩形内的数据。
    ///场景举例：订单区域画像分析的场景，想分析A小区购买力，而恰好这A小区是矩形的。我们通过统计A小区订单数量（或总价）即可。
    ///方法：在SearchQuery的中构造一个{@link BoolQuery},其 mustQueries 中放入一个{@link GeoBoundingBoxQuery}的矩形地理位置，然后mustQueries再放入查询订单数量的query，就可以获得想要的结果。
    /// </summary>
    public class GeoBoundingBoxQuery : IQuery
    {
        public string FieldName { get; set; }
        /**
         * 矩形的左上角的经纬度
         * <p>示例："46.24123424, 23.2342424"</p>
         */
        public string TopLeft { get; set; }
        /**
         * 矩形的右下角经纬度
         * <p>示例："46.24123424, 23.2342424"</p>
         */
        public string BottomRight { get; set; }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_GeoBoundingBoxQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildGeoBoundingBoxQuery(this).ToByteString();
        }
    }
}
