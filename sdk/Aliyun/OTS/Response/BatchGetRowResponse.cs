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
using System.Linq;

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// 表示BatchGetRow的返回。
    /// </summary>
    public class BatchGetRowResponse : OTSResponse
    {
        /// <summary>
        /// 每个表的返回数据。它是一个字段，Key是表名，Value是BatchGetRowResponseItem的List。
        /// 其中BatchGetRowResponseItem表示单行返回的结果。
        /// </summary>
        public IDictionary<string, IList<BatchGetRowResponseItem>> RowDataGroupByTable { get; private set; }

        public BatchGetRowResponse()
        {
            RowDataGroupByTable = new Dictionary<string, IList<BatchGetRowResponseItem>>();
        }

        public void Add(string tableName, IList<BatchGetRowResponseItem> rowDataInTable)
        {
            RowDataGroupByTable.Add(tableName, rowDataInTable);
        }

        public bool IsAllSucceed
        {
            get { return !GetFailedRows().Any(); }
        }

        public IEnumerable<BatchGetRowResponseItem> GetFailedRows()
        {
            var result = new List<BatchGetRowResponseItem>();
            foreach (var tableResult in RowDataGroupByTable)
            {
                result.AddRange(tableResult.Value.Where(_ => !_.IsOK));
            }

            return result;
        }
    }
}
