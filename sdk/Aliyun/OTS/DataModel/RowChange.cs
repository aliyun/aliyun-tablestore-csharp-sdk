using System;
using System.Diagnostics.Contracts;
namespace Aliyun.OTS.DataModel
{
    /**
 * 单行的数据变更操作的基础结构。
 * <p>若是PutRow操作，请参考{@link RowPutChange}。</p>
 * <p>若是UpdateRow操作，请参考{@link RowUpdateChange}。</p>
 * <p>若是DeleteRow操作，请参考{@link RowDeleteChange}。</p>
 */
    public abstract class RowChange : IRow, IMeasurable
    {
        /// <summary>
        /// 表的名称。
        /// </summary>
        /// <value>The name of the table.</value>
        public String TableName { get; set; }

        /// <summary>
        /// 表的主键。
        /// </summary>
        /// <value>The primary key.</value>
        public PrimaryKey PrimaryKey { get; set; }

        /// <summary>
        /// 判断条件。
        /// </summary>
        /// <value>The condition.</value>
        public Condition Condition { get; set; }

        /// <summary>
        /// 返回的数据类型，默认是不返回。
        /// </summary>
        /// <value>The type of the return.</value>
        public ReturnType ReturnType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Aliyun.OTS.DataModel.RowChange"/> class.
        /// <p>表的名称不能为null或者为空。</p>
        /// <p>行的主键不能为null或者为空。</p> 
        /// </summary>
        /// <param name="tableName">表的名称</param>
        /// <param name="primaryKey">表的主键</param>
        protected RowChange(string tableName, PrimaryKey primaryKey)
        {
            Contract.Requires(!string.IsNullOrEmpty(tableName), "表的名称不能为null或者为空。");
            this.TableName = tableName;
            this.PrimaryKey = primaryKey;
            this.Condition = new Condition();
            this.ReturnType = ReturnType.RT_NONE;
        }

        /// <summary>
        /// 构造函数。internal use
        /// <p>表的名称不能为null或者为空。</p>
        /// </summary>
        /// <param name="tableName">表的名称</param>
        protected RowChange(String tableName) : this(tableName, null)
        {
        }

        public int CompareTo(IRow row)
        {
            return this.PrimaryKey.CompareTo(row.GetPrimaryKey());
        }

        public PrimaryKey GetPrimaryKey()
        {
            return this.PrimaryKey;
        }

        public int GetDataSize()
        {
            throw new NotImplementedException();
        }
    }
}
