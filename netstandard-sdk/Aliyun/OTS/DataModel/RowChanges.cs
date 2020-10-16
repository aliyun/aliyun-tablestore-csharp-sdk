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

using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 用来表示<see cref="OTSClient.BatchWriteRow"/>中同一个表的多行操作，包括Put, Update和Delete。
    /// </summary>
    public class RowChanges
    {
        /// <summary>
        /// 表示多个Put操作。
        /// </summary>
        public List<Tuple<Condition, PrimaryKey, AttributeColumns>> PutOperations { get; set; }
        
        /// <summary>
        /// 表示多个Update操作。
        /// </summary>
        public List<Tuple<Condition, PrimaryKey, UpdateOfAttribute>> UpdateOperations { get; set; }
        
        /// <summary>
        /// 表示多个Delete操作。
        /// </summary>
        public List<Tuple<Condition, PrimaryKey>> DeleteOperations { get; set;}


        /// <summary>
        /// 表示多个Put操作。
        /// </summary>
        public List<RowPutChange> RowPutChanges;

        /// <summary>
        /// 表示多个Update操作。
        /// </summary>
        public List<RowUpdateChange> RowUpdateChanges;

        /// <summary>
        /// 表示多个Delete操作。
        /// </summary>
        public List<RowDeleteChange> RowDeleteChanges;

        public string TableName { get; set;}
        
        public RowChanges(string tableName)
        {
            RowPutChanges = new List<RowPutChange>();
            RowUpdateChanges = new List<RowUpdateChange>();
            RowDeleteChanges = new List<RowDeleteChange>();
            TableName = tableName;
        }

        /// <summary>
        /// 添加一个Put操作
        /// </summary>
        /// <param name="condition">检查条件</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="attributeColumns">属性</param>
        public void AddPut(Condition condition, PrimaryKey primaryKey, AttributeColumns attributeColumns)
        {
            var item = new RowPutChange(TableName, primaryKey)
            {
                Condition = condition
            };

            if (attributeColumns != null)
            {
                foreach (var column in attributeColumns)
                {
                    item.AddColumn(column.Key, column.Value);
                }
            }

            RowPutChanges.Add(item);
        }
        
        /// <summary>
        /// 添加一个Delete操作
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="primaryKey"></param>
        public void AddDelete(Condition condition, PrimaryKey primaryKey)
        {
            var item = new RowDeleteChange(TableName, primaryKey)
            {
                Condition = condition
            };

            RowDeleteChanges.Add(item);
        }
        
        /// <summary>
        /// 添加一个Update操作
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="primaryKey"></param>
        /// <param name="updateAttributes"></param>
        public void AddUpdate(Condition condition, PrimaryKey primaryKey, UpdateOfAttribute updateAttributes)
        {
            var item = new RowUpdateChange(TableName, primaryKey)
            {
                Condition = condition
            };

            item.FromUpdateOfAtrribute(updateAttributes);

            RowUpdateChanges.Add(item);
        }

        public bool IsEmpty()
        {
            return RowPutChanges.Count + RowUpdateChanges.Count + RowDeleteChanges.Count == 0;
        }
    }
}
