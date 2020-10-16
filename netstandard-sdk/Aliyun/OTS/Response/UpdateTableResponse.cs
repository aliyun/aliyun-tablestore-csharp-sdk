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
    /// 表示UpdateTable的返回。
    /// </summary>
    public class UpdateTableResponse : OTSResponse
    {
        /// <summary>
        /// UpdateTable接口返回的预留读写吞吐量的详情。
        /// </summary>
        public ReservedThroughputDetails ReservedThroughputDetails { get; private set; }
        
        public UpdateTableResponse() {}

        public UpdateTableResponse(ReservedThroughputDetails reservedThroughputDetails)
        {
            ReservedThroughputDetails = reservedThroughputDetails;
        }
    }
}
