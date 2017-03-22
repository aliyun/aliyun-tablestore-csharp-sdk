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
    /// 表示 BatchWriteRow 操作中每一行的操作状态。
    /// </summary>
    public class BatchWriteRowResponseItem
    {
        
        /// <summary>
        /// 操作是否成功
        /// </summary>
        public bool IsOK = true;
        
        /// <summary>
        /// 错误码
        /// </summary>
        public string ErrorCode = null;
        
        /// <summary>
        /// 错误消息
        /// </summary>
        public string ErrorMessage = null;

        /// <summary>
        /// 表名
        /// </summary>
        public string TableName;

        /// <summary>
        /// 更新编号
        /// </summary>
        public int Index = 0;

        /// <summary>
        /// 本次操作消耗的读写能力单元
        /// </summary>
        public CapacityUnit Consumed { get; set; }
        
        public BatchWriteRowResponseItem(string errorCode, string errorMessage, string tableName, int index)
        {
            IsOK = false;
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
            TableName = tableName;
            Index = index;
        }
        
        public BatchWriteRowResponseItem(CapacityUnit consumed, string tableName, int index)
        {
            IsOK = true;
            Consumed = consumed;
            TableName = tableName;
            Index = index;
        }
    }
}
