using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.SQL
{
    public class SQLTableMeta
    {
        /// <summary>
        /// 表的字段定义
        /// </summary>
        public List<SQLColumnSchema> Schema { get; set; }
        /// <summary>
        /// 表字段名到表字段下表的映射表
        /// </summary>
        public Dictionary<string, int?> ColumnsMap { get; set; }

        public SQLTableMeta(List<SQLColumnSchema> schema, Dictionary<string, int?> columnsMap)
        {
            if (schema != null)
            {
                Schema = schema;
            }

            if (columnsMap != null)
            {
                ColumnsMap = columnsMap;
            }
        }

        public List<SQLColumnSchema> GetSchema()
        {
            return Schema;
        }

        public Dictionary<string, int?> GetColumnsMap()
        {
            return ColumnsMap;
        }
    }
}
