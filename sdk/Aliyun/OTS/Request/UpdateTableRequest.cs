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
    /// 表示一个UpdateTable请求。
    /// </summary>
    public class UpdateTableRequest : OTSRequest
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { get; set; }
        
        /// <summary>
        /// 要更新的预留读写吞吐量。这里的预留读写吞吐量可以仅设置读或写。
        /// </summary>
        public CapacityUnit ReservedThroughput { get; set; }
        
        public UpdateTableRequest(string tableName, CapacityUnit reservedThroughput)
        {
            TableName = tableName;
            ReservedThroughput = reservedThroughput;
        }
    }
}
