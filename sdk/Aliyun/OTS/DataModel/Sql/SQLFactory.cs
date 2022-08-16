using System;
using com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers;
using Google.FlatBuffers;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.SQL
{
    public class SQLFactory
    {
        public static ISQLResultSet GetSQLResultSet(SQLPayloadVersion? version, SQLStatementType? type, ByteString rows)
        {
            if (!type.HasValue)
            {
                return null;
            }

            switch (type.Value)
            {
                case SQLStatementType.SQLSelect:
                case SQLStatementType.SQLShowTable:
                case SQLStatementType.SQLDescribeTable:
                    if (!version.HasValue)
                    {
                        return null;
                    }
                    return new SQLResultSetImpl(version.Value, rows);
                case SQLStatementType.SQLCreateTable:
                case SQLStatementType.SQLDropTable:
                case SQLStatementType.SQLAlterTable:
                default:
                    return null;
            }
        }

        public static ISQLRows GetSQLRows(SQLPayloadVersion version, ByteString rows)
        {
            switch (version)
            {
                case SQLPayloadVersion.SQLFlatBuffers:
                    if (rows.IsEmpty)
                    {
                        throw new ArgumentNullException("SQL response get rows should not be null");
                    }

                    ByteBuffer rowBuffer = new ByteBuffer(rows.ToByteArray());
                    SQLResponseColumns columns = SQLResponseColumns.GetRootAsSQLResponseColumns(rowBuffer);
                    return new SQLRowsFBsColumnBased(columns);
                default:
                    throw new NotSupportedException(string.Format("Do not support other SQL payload version: {0}", version));
            }
        }

        public static ISQLRow GetSQLRow(ISQLRows sqlRows, int rowIndex)
        {
            return new SQLRowImpl(sqlRows, rowIndex);
        }
    }
}
