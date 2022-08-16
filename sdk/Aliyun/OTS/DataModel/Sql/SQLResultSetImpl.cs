using Google.ProtocolBuffers;
using System;

namespace Aliyun.OTS.DataModel.SQL
{
    /// <summary>
    /// 表示SQL表的数据返回集合
    /// </summary>
    public class SQLResultSetImpl : ISQLResultSet
    {
        public ISQLRows SQLRows { get; set; }

        private int Current = 0;

        public SQLResultSetImpl(SQLPayloadVersion version, ByteString rows)
        {
            SQLRows = SQLFactory.GetSQLRows(version, rows);
        }

        public SQLTableMeta GetSQLTableMeta()
        {
            return SQLRows.GetSQLTableMeta();
        }

        public bool HasNext()
        {
            return Current < SQLRows.GetRowCount();
        }

        public ISQLRow Next()
        {
            if (!HasNext())
            {
                throw new ArgumentOutOfRangeException("SQLRow doesn't have next row");
            }

            ISQLRow sqlRow = SQLFactory.GetSQLRow(SQLRows, Current);
            Current++;
            return sqlRow;
        }

        public long RowCount()
        {
            return SQLRows.GetRowCount();
        }

        public bool Absolute(int rowIndex)
        {
            if (rowIndex >= SQLRows.GetRowCount() || rowIndex < 0)
            {
                return false;
            }

            Current = rowIndex;
            return true;
        }
    }
}
