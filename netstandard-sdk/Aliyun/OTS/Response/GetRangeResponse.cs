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

namespace Aliyun.OTS.Response
{   
    /// <summary>
    /// 表示GetRange的返回
    /// </summary>
    public class GetRangeResponse : OTSResponse
    {
        /// <summary>
        /// 本次操作消耗的读写能力单元。
        /// </summary>
        public CapacityUnit ConsumedCapacityUnit { get; set; }
        
        /// <summary>
        /// 下一个主键，用于继续读取。
        /// </summary>
        public PrimaryKey NextPrimaryKey { get; set; }
        
        /// <summary>
        /// 返回的每一行数据
        /// </summary>
        public IList<Row> RowDataList { get; set; }

        public byte[] NextToken { get; set; }

        public GetRangeResponse() 
        {
            RowDataList = new List<Row>();
        }
    }
}
