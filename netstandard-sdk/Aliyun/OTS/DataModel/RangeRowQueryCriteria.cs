/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 */

using Aliyun.OTS.Request;

namespace Aliyun.OTS.DataModel
{
    public class RangeRowQueryCriteria : RowQueryCriteria
    {
        /// <summary>
        /// 设置或获取范围查询的读取顺序（正序(FORWARD)或反序(BACKWARD)）。
        /// </summary>
        public GetRangeDirection Direction { get; set; }

        /// <summary>
        /// 设置或获取查询返回的最大行数，若limit未设置，则返回查询范围下的所有行
        /// </summary>
        public int? Limit { get; set; }

        /// <summary>
        ///  设置或获取范围查询的左边界的主键值
        /// </summary>
        public PrimaryKey InclusiveStartPrimaryKey { get; set; }

        /// <summary>
        /// 设置或获取获取范围查询的右边界的主键值
        /// </summary>
        public PrimaryKey ExclusiveEndPrimaryKey { get; set; }

        /// <summary>
        /// 用于行内流式读, 标记位置和状态信息.
        /// </summary>
        public byte[] Token { get; set; }

        /// <summary>
        /// 构造一个在给定名称的表中查询的条件。
        /// </summary>
        /// <param name="tableName">表名称</param>
        public RangeRowQueryCriteria(string tableName)
            : base(tableName)
        {
            Direction = GetRangeDirection.Forward;
            InclusiveStartPrimaryKey = new PrimaryKey();
            ExclusiveEndPrimaryKey = new PrimaryKey();
        }

        public bool HasSetToken()
        {
            return Token != null;
        }
    }
}
