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
    /// 表示一个CreateTable的请求。
    /// </summary>
    public class CreateTableRequest : OTSRequest
    {
        /// <summary>
        /// 表的元数据，包含表名和主键的设计。
        /// </summary>
        public TableMeta TableMeta { get; set; }
        
        /// <summary>
        /// 预留读写吞吐量。
        /// </summary>
        public CapacityUnit ReservedThroughput { get; set; }
        public CreateTableRequest(TableMeta tableMeta, CapacityUnit reservedThroughput) 
        {
            TableMeta = tableMeta;
            ReservedThroughput = reservedThroughput;
        }
    }
}
