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

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表示若干属性列组成的属性。可以使用Add方法指定列名和列值来添加属性列。
    /// </summary>
    public class AttributeColumns : Dictionary<string, ColumnValue>
    {
    }
}
