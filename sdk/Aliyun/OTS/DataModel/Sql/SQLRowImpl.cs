using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Aliyun.OTS.DataModel.SQL
{
    public class SQLRowImpl : ISQLRow
    {
        public ISQLRows SQLRows { get; set; }

        public int RowIndex { get; set; }

        public SQLRowImpl(ISQLRows sqlRows, int rowIndex)
        {
            SQLRows = sqlRows;

            RowIndex = rowIndex;
        }

        public object Get(int columnIndex)
        {
            CheckValid(columnIndex, null);

            return SQLRows.GetObject(RowIndex, columnIndex);
        }

        public object Get(string name)
        {
            CheckValid(name, null);

            int? rowIndex = SQLRows.GetSQLTableMeta().ColumnsMap[name];

            if (rowIndex.HasValue)
            {
                return Get((int)rowIndex);
            }

            return null;
        }

        public string GetString(int columnIndex)
        {
            CheckValid(columnIndex, ColumnValueType.String);

            Object value = Get(columnIndex);

            return (string)value;
        }

        public string GetString(string name)
        {
            CheckValid(name, ColumnValueType.String);

            Object value = Get(name);

            return (string)value;
        }

        public long? GetLong(int columnIndex)
        {
            CheckValid(columnIndex, ColumnValueType.Integer);

            Object value = Get(columnIndex);

            return (long?)value;
        }

        public long? GetLong(string name)
        {
            CheckValid(name, ColumnValueType.Integer);

            Object value = Get(name);

            return (long?)value;
        }

        public bool? GetBoolean(int columnIndex)
        {
            CheckValid(columnIndex, ColumnValueType.Boolean);

            Object value = Get(columnIndex);

            return (bool?)value;
        }

        public bool? GetBoolean(string name)
        {
            CheckValid(name, ColumnValueType.Boolean);

            Object value = Get(name);

            return (bool?)value;
        }

        public double? GetDouble(int columnIndex)
        {
            CheckValid(columnIndex, ColumnValueType.Double);

            Object value = Get(columnIndex);

            return (double?)value;
        }

        public double? GetDouble(string name)
        {
            CheckValid(name, ColumnValueType.Double);

            Object value = Get(name);

            return (double?)value;
        }

        public MemoryStream GetBinary(int columnIndex)
        {
            CheckValid(columnIndex, ColumnValueType.Binary);

            Object value = Get(columnIndex);

            return (MemoryStream)value;
        }

        public MemoryStream GetBinary(string name)
        {
            CheckValid(name, ColumnValueType.Binary);
            Object value = Get(name);

            return (MemoryStream)value;
        }

        public string ToDebugString()
        {
            StringBuilder sb = new StringBuilder();

            List<SQLColumnSchema> schemas = SQLRows.GetSQLTableMeta().GetSchema();

            if (schemas != null)
            {
                for (int i = 0; i < schemas.Count; i++)
                {
                    sb.Append(schemas[i].Name + ":");
                    sb.Append(Get(i));
                    if (i < schemas.Count - 1)
                    {
                        sb.Append(", ");
                    }
                }
            }
            return sb.ToString();
        }

        private void CheckValid(int columnIndex, ColumnValueType? responseType)
        {
            if (columnIndex >= SQLRows.GetColumnCount() || columnIndex < 0)
            {
                throw new ArgumentException(string.Format("Column index {0} is out of range", columnIndex));
            }

            if (responseType.HasValue && responseType != SQLRows.GetSQLTableMeta().GetSchema()[columnIndex].ColumnType)
            {
                throw new ArgumentException(string.Format("Column type collates failed, response type : {0}, but the real is: {1}",
                    responseType,
                    SQLRows.GetSQLTableMeta().GetSchema()[columnIndex].ColumnType));
            }
        }

        private void CheckValid(string name, ColumnValueType? responseType)
        {
            if (!SQLRows.GetSQLTableMeta().ColumnsMap.ContainsKey(name))
            {
                throw new ArgumentException(string.Format("SQLRow doesn't contains field name: {0}", name));
            }

            int columnIndex = (int)SQLRows.GetSQLTableMeta().ColumnsMap[name];

            if (responseType.HasValue && responseType != SQLRows.GetSQLTableMeta().Schema[columnIndex].ColumnType)
            {
                throw new ArgumentException(string.Format("Column type collates failed, response type: {0}, but the real is: {1}",
                    responseType,
                    SQLRows.GetSQLTableMeta().Schema[columnIndex].ColumnType));
            }
        }
    }
}
