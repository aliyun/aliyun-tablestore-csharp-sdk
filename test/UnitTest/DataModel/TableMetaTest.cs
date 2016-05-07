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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Net;
using System.Net.Http;
using System.IO;

using NUnit.Framework;

using Aliyun.OTS;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;

namespace Aliyun.OTS.UnitTest.DataModel
{
    [TestFixture]
    class TableMetaTest : OTSUnitTestBase
    {
        // <summary>
        // 测试CreateTable和DescribeTable在TableMeta包含2个PK，类型为 INTEGER 的情况。
        // </summary>
        [Test]
        public void TestIntegerPKInSchema() 
        {
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.Integer);
            primaryKeySchema.Add("PK1", ColumnValueType.Integer);
            
            SetTestConext(pkSchema:primaryKeySchema);
            
            TestSingleAPI("CreateTable");
            TestSingleAPI("DescribeTable");
        }

        // <summary>
        // 测试CreateTable和DescribeTable在TableMeta包含2个PK，类型为 STRING 的情况。
        // </summary>
        [Test]
        public void TestStringPKInSchema() 
        {
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.String);
            primaryKeySchema.Add("PK1", ColumnValueType.String);
            
            SetTestConext(pkSchema:primaryKeySchema);
            
            TestSingleAPI("CreateTable");
            TestSingleAPI("DescribeTable");
        }

        // <summary>
        // 测试CreateTable和DescribeTable在TableMeta包含2个PK，类型为 DOUBLE / BOOELAN / INF_MIN / INF_MAX 的情况，期望返回错误。
        // </summary>
        [Test]
        public void TestInvalidPKInSchema() 
        {
            // TODO Error Injection to test DescribeTable
            
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.Double);
            primaryKeySchema.Add("PK1", ColumnValueType.Double);
            
            SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"DOUBLE is an invalid type for the primary key.");
            TestSingleAPI("CreateTable");
            
            primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.Boolean);
            primaryKeySchema.Add("PK1", ColumnValueType.Boolean);
            
            SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"BOOLEAN is an invalid type for the primary key.");
            TestSingleAPI("CreateTable");
            
            //primaryKeySchema = new PrimaryKeySchema();
            //primaryKeySchema.Add("PK0", ColumnValueType.Binary);
            //primaryKeySchema.Add("PK1", ColumnValueType.Binary);
            
            //SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"BINARY is an invalid type for the primary key.");
            //TestSingleAPI("CreateTable");
            
            // INF_MIN INF_MAX 类型的 ColumnValueType 在C# SDK里没有
        }
    }
}
