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
using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示一个BatchWriteRow的请求。先构造实例，再使用Add方法添加写请求。
    /// </summary>
    public class BatchWriteRowRequest : OTSRequest
    {
        
        /// <summary>
        /// 表示BatchWriteRow请求中每个表要修改的数据。
        /// </summary>
        public IDictionary<string, RowChanges> RowChangesGroupByTable { get; private set; }

        public BatchWriteRowRequest() 
        {
            RowChangesGroupByTable = new Dictionary<string, RowChanges>();
        }

        /// <summary>
        /// 添加一个表中要修改的数据。
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="rowChanges">多行修改</param>
        public void Add(string tableName, RowChanges rowChanges)
        {
            RowChangesGroupByTable.Add(tableName, rowChanges);
        }
    }
}
