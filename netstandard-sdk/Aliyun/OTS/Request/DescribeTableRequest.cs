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


namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示一个DescribeTable请求。
    /// </summary>
    public class DescribeTableRequest : OTSRequest
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { get; set; }
        public DescribeTableRequest(string tableName) 
        {
            TableName = tableName;
        }
    }
}
