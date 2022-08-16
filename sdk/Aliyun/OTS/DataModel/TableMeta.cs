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
    /// 表的元信息。建表时需要设定TableMeta，DescribeTable时可以返回这个TableMeta。
    /// </summary>
    public class TableMeta
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// 主键的设计，包括每一个主键列的列名和列值类型，有序。
        /// </summary>
        public PrimaryKeySchema PrimaryKeySchema { get; set; }

        /// <summary>
        /// 预定义列
        /// </summary>
        public DefinedColumnSchema DefinedColumnSchema { get; set; }

        public TableMeta(string tableName, PrimaryKeySchema primaryKeySchema)
        {
            TableName = tableName;
            PrimaryKeySchema = primaryKeySchema;
        }

        public TableMeta(string tableName, DefinedColumnSchema definedColumnSchema)
        {
            TableName = tableName;
            DefinedColumnSchema = definedColumnSchema;
        }

        public TableMeta(string tableName, PrimaryKeySchema primaryKeySchema, DefinedColumnSchema definedColumnSchema)
        {
            TableName = tableName;
            PrimaryKeySchema = primaryKeySchema;
            DefinedColumnSchema = definedColumnSchema;
        }
    }
}
