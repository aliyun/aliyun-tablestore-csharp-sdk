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
using System.Linq;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.ConditionalUpdate;

namespace Aliyun.OTS.Request
{
    public struct BatchGetRowRequestItem
    {
        public string TableName;
        public IList<PrimaryKey> PrimaryKeys;
        public HashSet<string> ColumnsToGet;
    }

    /// <summary>
    /// 表示一个BatchGetRow的请求。先构造它的实例，再使用Add方法添加读请求。
    /// </summary>
    public class BatchGetRowRequest : OTSRequest
    {
        private readonly IDictionary<string, MultiRowQueryCriteria> rowQueryCriteriaDict;

        /// <summary>
        /// 构造一个新的<see cref="BatchGetRowRequest"/>
        /// </summary>
        public BatchGetRowRequest() 
        {
            rowQueryCriteriaDict = new Dictionary<string, MultiRowQueryCriteria>();
        }

        /// <summary>
        /// 添加一个表的多行读条件
        /// </summary>
        /// <param name="rowQueryCriteria">多行读的条件语句</param>
        public void Add(MultiRowQueryCriteria rowQueryCriteria)
        {
            if (rowQueryCriteria != null && !string.IsNullOrEmpty(rowQueryCriteria.TableName))
            {
                rowQueryCriteriaDict[rowQueryCriteria.TableName] = rowQueryCriteria;
            }
        }
        
        /// <summary>
        /// 添加一个表的多行读请求。
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKeys">多行的主键</param>
        /// <param name="columnsToGet">要读取的列</param>
        /// <param name="condition">过滤条件</param>
        public void Add(string tableName, 
                        List<PrimaryKey> primaryKeys, 
                        HashSet<string> columnsToGet = null,
                        IColumnCondition condition = null)
        {
            var rowQueryCriteria = new MultiRowQueryCriteria(tableName);
            rowQueryCriteria.SetRowKeys(primaryKeys);

            if (columnsToGet != null)
            {
                rowQueryCriteria.SetColumnsToGet(columnsToGet);
            }

            if (condition != null)
            {
                rowQueryCriteria.Filter = condition.ToFilter();
            }

            rowQueryCriteriaDict[tableName] = rowQueryCriteria;
        }

        /// <summary>
        /// 按照表名称获取<see cref="MultiRowQueryCriteria"/>
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <returns>多行查询条件</returns>
        public MultiRowQueryCriteria GetCriteriasByTable(string tableName)
        {
            if (rowQueryCriteriaDict.ContainsKey(tableName))
            {
                return rowQueryCriteriaDict[tableName];
            }

            return null;
        }

        /// <summary>
        /// 获取<see cref="MultiRowQueryCriteria"/>列表
        /// </summary>
        /// <returns></returns>
        public IList<MultiRowQueryCriteria> GetCriterias()
        {
            return rowQueryCriteriaDict.Values.ToList();
        }

        /// <summary>
        /// 表示BatchGetRow请求中每个表的行。
        /// </summary>
        [Obsolete("ItemList is deprecated, please use GetCriterias instead.")]
        public IList<BatchGetRowRequestItem> ItemList
        {
            get
            {
                IList<BatchGetRowRequestItem> itemList = new List<BatchGetRowRequestItem>();

                foreach (KeyValuePair<string, MultiRowQueryCriteria> criteria in rowQueryCriteriaDict)
                {
                    var item = new BatchGetRowRequestItem
                    {
                        TableName = criteria.Value.TableName,
                        PrimaryKeys = criteria.Value.GetRowKeys(),
                        ColumnsToGet = criteria.Value.GetColumnsToGet()
                    };

                    itemList.Add(item);
                }

                return itemList;
            }
        }
    }
}
