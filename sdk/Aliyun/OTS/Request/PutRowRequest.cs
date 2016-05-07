/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */


using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示一个PutRow请求。
    /// </summary>
    public class PutRowRequest : OTSRequest
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { get; set; }
        
        /// <summary>
        /// 操作条件
        /// </summary>
        public Condition Condition { get; set; }
        
        /// <summary>
        /// 主键
        /// </summary>
        public PrimaryKey PrimaryKey { get; set; }
        
        /// <summary>
        /// 属性
        /// </summary>
        public AttributeColumns Attribute { get; set; }
        
        public PutRowRequest(string tableName, Condition condition, PrimaryKey primaryKey, AttributeColumns attribute)
        {
            TableName = tableName;
            Condition = condition;
            PrimaryKey = primaryKey;
            Attribute = attribute;
        }
    }
}
