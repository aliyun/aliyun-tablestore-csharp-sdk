using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.SQL;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// SQL请求的返回结果
    /// </summary>
    public class SQLQueryResponse : OTSResponse
    {
        public Dictionary<string, ConsumedCapacity> ConsumedCapacityByTable { get; set; }

        public SQLPayloadVersion? SQLPayloadVersion { get; set; }

        public SQLStatementType? SQLStatementType { get; set; }

        public ByteString Rows { get; set; }

        public SQLQueryResponse()
        {
            SQLPayloadVersion = DataModel.SQL.SQLPayloadVersion.SQLFlatBuffers;
        }

        public SQLQueryResponse(Dictionary<string, ConsumedCapacity> consumedCapacityByTable, SQLPayloadVersion version, SQLStatementType type, ByteString rows)
        {
            if (consumedCapacityByTable != null)
            {
                ConsumedCapacityByTable = consumedCapacityByTable;
            }

            SQLStatementType = type;
            SQLPayloadVersion = version;
            Rows = rows;
        }

        public ISQLResultSet GetSQLResultSet()
        {
            return SQLFactory.GetSQLResultSet(SQLPayloadVersion, SQLStatementType, Rows);
        }
    }
}
