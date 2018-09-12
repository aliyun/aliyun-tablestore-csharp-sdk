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
using NUnit.Framework;
using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.UnitTest.DataModel
{

    [TestFixture]
    class ColumnNameTest : OTSUnitTestBase
    {
        public void TestBadColumnName(string badColumnName)
        {
            var badPrimaryKeySchema = new PrimaryKeySchema
            {
                { badColumnName, ColumnValueType.String }
            };

            var badPrimaryKey = new PrimaryKey
            {
                { badColumnName, new ColumnValue(3.14) }
            };

            var badColumnsToGet = new HashSet<string>
            {
                badColumnName
            };

            var expectFailureInfo = "Invalid column name: '" + badColumnName + "'.";

            var expectedFailure = new Dictionary<string, string>
            {
                { "CreateTable", expectFailureInfo}
            };

            var errorMessage = String.Format("Bug: unsupported primary key type: Double");
            var badAttribute = new AttributeColumns
            {
                { badColumnName, new ColumnValue(3.14) }
            };

            SetTestConext(
                pkSchema: badPrimaryKeySchema,
                primaryKey: badPrimaryKey,
                startPrimaryKey: badPrimaryKey,
                expectedFailure: expectedFailure,
                allFailedMessage: errorMessage);
            TestAllDataAPI(deleteTable: false);

            SetTestConext(
                attribute: badAttribute,
                allFailedMessage: expectFailureInfo);
            TestAllDataAPIWithAttribute(false);

            SetTestConext(
                columnsToGet: badColumnsToGet,
                expectedFailure: expectedFailure,
                allFailedMessage: expectFailureInfo);
            TestAllDataAPIWithColumnsToGet();
        }
        
        // <summary>
        // 测试所有接口，列名长度为0的情况，期望返回错误消息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        // </summary>
        [Test]
        public void TestColumnNameOfZeroLength() 
        {
            TestBadColumnName("");
        }

        //// <summary>
        //// 测试所有接口，列名包含Unicode，期望返回错误信息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        //// </summary>
        //[Test]
        //public void TestColumnNameWithUnicode() 
        //{
        //    TestBadColumnName("中文");
        //}

        // <summary>
        // 测试所有接口，列名长度为1KB，期望返回错误信息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        // </summary>
        [Test]
        public void Test1KBColumnName() 
        {
            TestBadColumnName(new string('X', 1024));
        }
    }
}
