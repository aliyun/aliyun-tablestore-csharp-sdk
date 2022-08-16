namespace Aliyun.OTS.DataModel.SQL
{
    /// <summary>
    /// 表示SQL表的列结构信息
    /// </summary>
    public class SQLColumnSchema
    {
        /// <summary>
        /// 列名
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 列的数据类型
        /// </summary>
        public ColumnValueType? ColumnType { get; set; }

        public SQLColumnSchema(string name, ColumnValueType columnType)
        {
            Name = name;
            ColumnType = columnType;
        }
    }
}
