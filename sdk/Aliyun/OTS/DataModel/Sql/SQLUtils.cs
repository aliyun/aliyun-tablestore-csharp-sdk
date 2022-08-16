using Aliyun.OTS.Response;
using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.SQL
{
    public class SQLUtils
    {
        public static List<string> ParseShowTablesResponse(SQLQueryResponse response)
        {
            if (response.SQLStatementType != SQLStatementType.SQLShowTable)
            {
                throw new ArgumentException(string.Format("SQL statement is not {0}.", SQLStatementType.SQLShowTable.ToString()));
            }

            ISQLResultSet rs = response.GetSQLResultSet();
            List<string> tables = new List<string>();
            while (rs.HasNext())
            {
                tables.Add(rs.Next().GetString(0));
            }

            return tables;
        }
    }
}
