using System;
namespace Aliyun.OTS.DataModel
{
    public class RowDeleteChange : RowChange
    {
        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="tableName">表的名称</param>
        /// <param name="primaryKey"> 要删除的行的主键</param>
        public RowDeleteChange(String tableName, PrimaryKey primaryKey) : base(tableName, primaryKey)
        {
        }

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="tableName">表的名称</param>
        public RowDeleteChange(String tableName) : base(tableName)
        {
        }

        public new int GetDataSize()
        {
            return GetPrimaryKey().GetDataSize();
        }
    }
}
