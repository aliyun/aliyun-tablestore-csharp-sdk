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
        public List<Tuple<Condition, PrimaryKey, AttributeColumns>> PutOperations;
        
        /// <summary>
        /// 表示多个Update操作。
        /// </summary>
        public List<Tuple<Condition, PrimaryKey, UpdateOfAttribute>> UpdateOperations;
        
        /// <summary>
        /// 表示多个Delete操作。
        /// </summary>
        public List<Tuple<Condition, PrimaryKey>> DeleteOperations;
        
        public RowChanges()
        {
            PutOperations = new List<Tuple<Condition, PrimaryKey, AttributeColumns>>();
            UpdateOperations = new List<Tuple<Condition, PrimaryKey, UpdateOfAttribute>>();
            DeleteOperations = new List<Tuple<Condition, PrimaryKey>>();
        }

        /// <summary>
        /// 添加一个Put操作
        /// </summary>
        /// <param name="condition">检查条件</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="attribute">属性</param>
        public void AddPut(Condition condition, PrimaryKey primaryKey, AttributeColumns attribute)
        {
            var item = new Tuple<Condition, PrimaryKey, AttributeColumns>(condition, primaryKey, attribute);
            PutOperations.Add(item);
        }
        
        /// <summary>
        /// 添加一个Delete操作
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="primaryKey"></param>
        public void AddDelete(Condition condition, PrimaryKey primaryKey)
        {
            var item = new Tuple<Condition, PrimaryKey>(condition, primaryKey);
            DeleteOperations.Add(item);
        }
        
        /// <summary>
        /// 添加一个Update操作
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="primaryKey"></param>
        /// <param name="updates"></param>
        public void AddUpdate(Condition condition, PrimaryKey primaryKey, UpdateOfAttribute updates)
        {
            var item = new Tuple<Condition, PrimaryKey, UpdateOfAttribute>(condition, primaryKey, updates);
            UpdateOperations.Add(item);
        }
    }
}
