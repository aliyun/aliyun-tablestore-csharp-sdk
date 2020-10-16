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

using System;
using System.Collections.Generic;
using System.Linq;

namespace Aliyun.OTS.Response
{
    public struct BatchWriteRowResponseForOneTable
    {
        [Obsolete("Please use Responses")]
        public IList<BatchWriteRowResponseItem> PutResponses;

        [Obsolete("Please use Responses")]
        public IList<BatchWriteRowResponseItem> UpdateResponses;

        [Obsolete("Please use Responses")]
        public IList<BatchWriteRowResponseItem> DeleteResponses;

        public IList<BatchWriteRowResponseItem> Responses;
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
        
        public bool IsAllSucceed {
            get { return !GetFailedRows().Any(); }
        }

        public IEnumerable<BatchWriteRowResponseItem> GetFailedRows() 
        {
            var result = new List<BatchWriteRowResponseItem>();
            foreach (var tableResponse in TableRespones)
            {
                result.AddRange(tableResponse.Value.Responses.Where(_ => !_.IsOK));
            }

            return result;
        }

        public BatchWriteRowResponse()
        {
            TableRespones = new Dictionary<string, BatchWriteRowResponseForOneTable>();
        }
    }
}
