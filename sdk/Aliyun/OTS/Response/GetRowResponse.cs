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
    /// 表示GetRow的返回
    /// </summary>
    public class GetRowResponse : OTSResponse
    {
        /// <summary>
        /// 本次操作消耗的读写能力单元。
        /// </summary>
        public CapacityUnit ConsumedCapacityUnit { get; private set; }
        
        /// <summary>
        /// 主键
        /// </summary>
        public PrimaryKey PrimaryKey { get; private set; }
        
        /// <summary>
        /// 属性
        /// </summary>
        public AttributeColumns Attribute { get; private set; }
        
        public GetRowResponse() {}

        public GetRowResponse(CapacityUnit consumedCapacityUnit, PrimaryKey primaryKey, AttributeColumns attribute)
        {
            ConsumedCapacityUnit = consumedCapacityUnit;
            PrimaryKey = primaryKey;
            Attribute = attribute;
        }
    }
}
