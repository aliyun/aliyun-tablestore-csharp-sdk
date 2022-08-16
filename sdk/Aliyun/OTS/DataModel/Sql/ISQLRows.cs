namespace Aliyun.OTS.DataModel.SQL
{
    /// <summary>
    /// 表示SQL数据存储的数据集
    /// </summary>
    public interface ISQLRows
    {
        /// <summary>
        /// 返回数据集的Schema
        /// </summary>
        /// <returns><see cref="SQLTableMeta"/></returns>
        SQLTableMeta GetSQLTableMeta();
        /// <summary>
        /// 返回数据集的总行数
        /// </summary>
        /// <returns><see cref="long"/></returns>
        long GetRowCount();
        /// <summary>
        /// 返回数据集的列数量
        /// </summary>
        /// <returns><see cref="long"/></returns>
        long GetColumnCount();
        /// <summary>
        /// 根据行游标和列游标查询某行某列的数据
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <param name="columnIndex"></param>
        /// <returns><see cref="object"/></returns>
        object GetObject(int rowIndex, int columnIndex);
    }
}
