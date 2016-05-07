/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 */

namespace Aliyun.OTS.DataModel
{
    public class SingleRowQueryCriteria : RowQueryCriteria
    {
        /// <summary>
        /// 设置和获取主键
        /// </summary>
        public PrimaryKey RowPrimaryKey {get;set;}

        /// <summary>
        /// 构造一个在给定名称的表中查询的条件。
        /// </summary>
        /// <param name="tableName">查询的表名</param>
        public SingleRowQueryCriteria(string tableName)
            : base(tableName)
        { }
    }
}
