namespace Aliyun.OTS.DataModel.SQL
{
    /// <summary>
    /// 表示SQL表达数据返回集合
    /// </summary>
    public interface ISQLResultSet
    {
        /// <summary>
        /// 返回数据集的Schema
        /// </summary>
        /// <returns><see cref="SQLTableMeta"/></returns>
        SQLTableMeta GetSQLTableMeta();
        /// <summary>
        /// 是否有下一条数据
        /// </summary>
        /// <returns><see cref="bool"/></returns>
        bool HasNext();
        /// <summary>
        /// 返回下一条数据
        /// </summary>
        /// <returns><see cref="ISQLRow"/></returns>
        ISQLRow Next();
        /// <summary>
        /// 返回总行数
        /// </summary>
        /// <returns><see cref="long"/></returns>
        long RowCount();
        /// <summary>
        /// 跳转到第rowIndex行
        /// </summary>
        /// <param name="rowIndex">游标</param>
        /// <returns>若跳转成功，返回true；若跳转失败（比如发生越界），返回false</returns>
        bool Absolute(int rowIndex);
    }
}
