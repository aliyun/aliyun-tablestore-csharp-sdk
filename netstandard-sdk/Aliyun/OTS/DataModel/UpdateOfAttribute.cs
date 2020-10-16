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
    /// 表示对属性的修改，用于<see cref="OTSClient.UpdateRow"/>接口。
    /// </summary>
    public class UpdateOfAttribute
    {
        /// <summary>
        /// 表示要删除的列的列名List。
        /// </summary>
        public IList<string> AttributeColumnsToDelete { get; private set; }
        
        /// <summary>
        /// 表示要写入的列的列名和列值。
        /// </summary>
        public IDictionary<string, ColumnValue> AttributeColumnsToPut { get; private set; }

        public UpdateOfAttribute()
        {
            AttributeColumnsToDelete = new List<string>();
            AttributeColumnsToPut = new Dictionary<string, ColumnValue>();
        }
        
        public void AddAttributeColumnToDelete(string columnName)
        {
            AttributeColumnsToDelete.Add(columnName);
        }
        
        public void AddAttributeColumnToPut(string columnName, ColumnValue columnValue)
        {
            AttributeColumnsToPut.Add(columnName, columnValue);
        }
    }
}
