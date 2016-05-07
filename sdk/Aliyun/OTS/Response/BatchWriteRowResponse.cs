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

using System.Collections.Generic;

namespace Aliyun.OTS.Response
{
    public struct BatchWriteRowResponseForOneTable
    {
        public IList<BatchWriteRowResponseItem> PutResponses;
        public IList<BatchWriteRowResponseItem> UpdateResponses;
        public IList<BatchWriteRowResponseItem> DeleteResponses;
    }

    /// <summary>
    /// 表示BatchWriteRow的返回。
    /// </summary>
    public class BatchWriteRowResponse : OTSResponse
    {
        /// <summary>
        /// 表示每一个表中的每一行的操作状态。
        /// </summary>
        public IDictionary<string, BatchWriteRowResponseForOneTable> TableRespones { get; private set; }
        
        public BatchWriteRowResponse()
        {
            TableRespones = new Dictionary<string, BatchWriteRowResponseForOneTable>();
        }
    }
}
