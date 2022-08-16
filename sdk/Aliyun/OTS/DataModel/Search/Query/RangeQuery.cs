using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 范围查询。通过设置一个范围（from，to），查询该范围内的所有数据。
    /// </summary>
    public class RangeQuery : IQuery
    {
        /// <summary>
        /// 字段名
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        /// 字段取值的下界
        /// </summary>
        public ColumnValue From { get; set; }
        /// <summary>
        /// 字段取值的上界
        /// </summary>
        public ColumnValue To { get; set; }
        /// <summary>
        ///  范围取值是否包含下界
        /// </summary>
        public bool IncludeLower { get; set; }
        /// <summary>
        /// 范围取值是否包含上界
        /// </summary>
        public bool IncludeUpper { get; set; }

        public RangeQuery()
        {
        }

        public RangeQuery(string fieldName, ColumnValue from, ColumnValue to)
        {
            FieldName = fieldName;
            From = from;
            To = to;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_RangeQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildRangeQuery(this).ToByteString();
        }
    }
}
