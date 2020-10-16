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
    /// 表示PutRow的返回
    /// </summary>
    public class PutRowResponse : OTSResponse
    {
        /// <summary>
        /// 本次操作消耗的读写能力单元。
        /// </summary>
        public CapacityUnit ConsumedCapacityUnit { get; private set; }

        /// <summary>
        /// 返回ReturnType指定的值
        /// </summary>
        public Row Row { get; set; }

        public PutRowResponse() { }

        public PutRowResponse(CapacityUnit consumedCapacityUnit)
        {
            ConsumedCapacityUnit = consumedCapacityUnit;
        }

        public PutRowResponse(CapacityUnit consumedCapacityUnit, Row row)
        {
            ConsumedCapacityUnit = consumedCapacityUnit;
            Row = row;
        }
    }
}
