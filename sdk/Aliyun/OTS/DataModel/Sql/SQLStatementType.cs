namespace Aliyun.OTS.DataModel.SQL
{
    public enum SQLStatementType
    {
        /// <summary>
        /// 查询
        /// </summary>
        SQLSelect,
        /// <summary>
        /// 建表
        /// </summary>
        SQLCreateTable,
        /// <summary>
        /// 查询表列表
        /// </summary>
        SQLShowTable,
        /// <summary>
        /// 查询表格式
        /// </summary>
        SQLDescribeTable,
        /// <summary>
        /// 删除表
        /// </summary>
        SQLDropTable,
        /// <summary>
        /// 修改表
        /// </summary>
        SQLAlterTable
    }
}
