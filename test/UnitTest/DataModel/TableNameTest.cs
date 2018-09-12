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

using NUnit.Framework;

namespace Aliyun.OTS.UnitTest.DataModel
{
    [TestFixture]
    class TableNameTest : OTSUnitTestBase
    {
        // <summary>
        // 测试所有接口，表名长度为0的情况，期望返回错误消息：Invalid table name: '{TableName}'. 中包含的TableName与输入一致。
        // </summary>
        [Test]
        public void TestTableNameOfZeroLength() 
        {
            SetTestConext(tableName:"", allFailedMessage:"Invalid table name: ''.");
            TestAllAPIWithTableName();
        }

        // <summary>
        // 测试所有接口，表名包含Unicode，期望返回错误信息：Invalid table name: '{TableName}'. 中包含的TableName与输入一致。
        // </summary>
        [Test]
        public void TestTableNameWithUnicode() 
        {
            SetTestConext(tableName:"中文表名", allFailedMessage:"Invalid table name: '中文表名'.");
            TestAllAPIWithTableName();
        }

        // <summary>
        // 测试所有接口，表名长度为1KB，期望返回错误信息：Invalid table name: '{TableName}'. 中包含的TableName与输入一致。
        // </summary>
        [Test]
        public void Test1KBTableName() 
        {
            string badTableName = new string('X', 1000);
            SetTestConext(tableName:badTableName, allFailedMessage:String.Format("Invalid table name: '{0}'.", badTableName));
            TestAllAPIWithTableName();
        }
    }
}
