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
    /// 表示若干主键列组成的主键。可以使用Add方法指定列名和列值来添加主键列。
    /// </summary>
    public class PrimaryKey : Dictionary<string, ColumnValue>
    {
    }
}
