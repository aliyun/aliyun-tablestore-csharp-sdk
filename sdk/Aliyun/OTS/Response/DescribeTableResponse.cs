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

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// 表示DescribeTable的返回。
    /// </summary>
    public class DescribeTableResponse : OTSResponse
    {
        /// <summary>
        /// 表的元数据
        /// </summary>
        public TableMeta TableMeta { get; set; }
        
        /// <summary>
        /// 预留读写吞吐量的详细信息
        /// </summary>
        public ReservedThroughputDetails ReservedThroughputDetails { get; set; }

        public TableOptions TableOptions { get; set; }


        public StreamDetails StreamDetails { get; set; }

        public System.Collections.Generic.List<byte[]> ShardSplits { get; set; }
        
        public DescribeTableResponse() {}
        
        public DescribeTableResponse(TableMeta tableMeta, ReservedThroughputDetails reservedThroughputDetails, TableOptions tableOptions)
        {
            TableMeta = tableMeta;
            ReservedThroughputDetails = reservedThroughputDetails;
            TableOptions = tableOptions;
        }
    }
}
