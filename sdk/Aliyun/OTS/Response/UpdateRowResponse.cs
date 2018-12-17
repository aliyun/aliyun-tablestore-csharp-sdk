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
    /// 表示UpdateRow的返回
    /// </summary>
    public class UpdateRowResponse : OTSResponse
    {        
        /// <summary>
        /// ReturnType指定返回的值
        /// </summary>
        public Row Row { get; set; }

        /// <summary>
        /// 本次操作消耗的读写能力单元。
        /// </summary>
        public CapacityUnit ConsumedCapacityUnit { get; private set; }
        
        public UpdateRowResponse() {}

        public UpdateRowResponse(CapacityUnit consumedCapacityUnit, IRow row)
        {
            ConsumedCapacityUnit = consumedCapacityUnit;
            Row = row as Row;
        }
    }
}
