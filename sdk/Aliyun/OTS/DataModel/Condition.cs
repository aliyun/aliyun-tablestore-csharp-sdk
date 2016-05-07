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

using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 写操作（包括<see cref="OTSClient.PutRow"/>、<see cref="OTSClient.DeleteRow"/>、
    /// <see cref="OTSClient.UpdateRow"/>和<see cref="OTSClient.BatchWriteRow"/>）
    /// 的检查条件。当检查条件满足时，响应的操作才会执行；否则操作出错。
    /// </summary>
    public class Condition
    {
        /// <summary>
        /// condition update的新接口
        /// </summary>
        public RowExistenceExpectation RowExistenceExpect { get; set; }
        public ColumnCondition ColumnCondition { get; set; }

        public Condition() { }

        public Condition(RowExistenceExpectation rowExist)
        {
            RowExistenceExpect = rowExist;
        }
    }
}
