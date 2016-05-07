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
    public class BatchGetRowSuccessResponseItem : BatchGetRowResponseItem
    {
        public BatchGetRowSuccessResponseItem(
            CapacityUnit consumedCapacityUnit,
            PrimaryKey primaryKey,
            AttributeColumns attribute)
            : base(consumedCapacityUnit, primaryKey, attribute) { }
    }
}
