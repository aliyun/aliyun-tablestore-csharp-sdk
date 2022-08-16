using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 通配符查询。支持 *（ 任意0或多个）和 ？（任意1个字符）。
    ///举例：名字字段是“name”，想查询名字中包含“龙”的人，就可以“* 龙*” ，但是效率可能不高。
    /// </summary>
    public class WildcardQuery : IQuery
    {
        public string FieldName { get; set; }
        public string Value { get; set; }
        public float Weight { get; set; }

        public WildcardQuery(string fieldName, string value)
        {
            FieldName = fieldName;
            Value = value;
            Weight = 1.0f;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_WildcardQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildWildcardQuery(this).ToByteString();
        }
    }
}
