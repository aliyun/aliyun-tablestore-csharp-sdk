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
    /// 表示BatchGetRow操作中每一行的结果。
    /// </summary>
    public class BatchGetRowResponseItem
    {
        /// <summary>
        /// 该行的读取是否成功。
        /// </summary>
        public bool IsOK = true;
        
        /// <summary>
        /// 错误码
        /// </summary>
        public string ErrorCode = null;
        
        /// <summary>
        /// 错误信息
        /// </summary>
        public string ErrorMessage = null;
        
        /// <summary>
        /// 本次操作消耗的读写能力单元
        /// </summary>
        public CapacityUnit Consumed { get; set; }
        
        /// <summary>
        /// 该行的主键
        /// </summary>
        public PrimaryKey PrimaryKey { get; set; }
        
        /// <summary>
        /// 属性
        /// </summary>
        public AttributeColumns Attribute { get; set; }


        public string TableName { get; set; }

        public IRow Row { get; set; }

        public int Index { get; set; }
        public byte[] NextToken { get; set; }
        
        public BatchGetRowResponseItem(string errorCode, string errorMessage)
        {
            IsOK = false;
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
        }
        
        public BatchGetRowResponseItem(CapacityUnit consumed, PrimaryKey primaryKey, AttributeColumns attribute)
        {
            Consumed = consumed;
            PrimaryKey = primaryKey;
            Attribute = attribute;
        }

        public BatchGetRowResponseItem(string tableName, IRow row, CapacityUnit consumed, int index)
        {
            TableName = tableName;
            Row = row;
            Consumed = consumed;
            Index = index;

            if (row != null)
            {
                PrimaryKey = row.GetPrimaryKey();
                Attribute = (row as Row).AttributeColumns;
            }
            else
            {
                PrimaryKey = new PrimaryKey();
                Attribute = new AttributeColumns();
            }
        }

        public BatchGetRowResponseItem(string tableName, IRow row, CapacityUnit consumed, int index, byte[] nextToken):
        this(tableName, row, consumed, index)
        {
            NextToken = nextToken;
        }

        public bool HasNextToken()
        {
            return (NextToken != null) && (NextToken.Length > 0);
        }
    }
}
