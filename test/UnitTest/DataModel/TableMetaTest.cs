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


using NUnit.Framework;
using Aliyun.OTS.DataModel;

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
            var primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.Integer },
                { "PK1", ColumnValueType.Integer }
            };

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
            var primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.String }
            };

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

            var primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.Double },
                { "PK1", ColumnValueType.Double }
            };

            SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"DOUBLE is an invalid type for the primary key.");
            TestSingleAPI("CreateTable");

            primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.Boolean },
                { "PK1", ColumnValueType.Boolean }
            };

            SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"BOOLEAN is an invalid type for the primary key.");
            TestSingleAPI("CreateTable");

            primaryKeySchema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.Binary },
                { "PK1", ColumnValueType.Binary }
            };
            SetTestConext(pkSchema:primaryKeySchema, allFailedMessage:"BINARY is an invalid type for the primary key.");
            TestSingleAPI("CreateTable");
            
            // INF_MIN INF_MAX 类型的 ColumnValueType 在C# SDK里没有
        }
    }
}
