using Aliyun.OTS.DataModel.SQL;

namespace Aliyun.OTS.Request
{
    public class SQLQueryRequest : OTSRequest
    {
        /// <summary>
        /// 单行查询条件
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// 序列化格式
        /// </summary>
        public SQLPayloadVersion? SQLPayloadVersion { get; private set; }

        public SQLQueryRequest(string query) : this(query, DataModel.SQL.SQLPayloadVersion.SQLFlatBuffers)
        {
        }

        public SQLQueryRequest(string query, SQLPayloadVersion sqlPayloadVersion)
        {
            Query = query;
            SQLPayloadVersion = sqlPayloadVersion;
        }
    }
}
