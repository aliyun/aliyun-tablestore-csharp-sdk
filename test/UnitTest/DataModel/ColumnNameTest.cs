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
    class ColumnNameTest : OTSUnitTestBase
    {
        void testBadColumnName(string badColumnName)
        {
            var badPrimaryKeySchema = new PrimaryKeySchema();
            badPrimaryKeySchema.Add(badColumnName, ColumnValueType.String);
            
            var badPrimaryKey = new PrimaryKey();
            badPrimaryKey.Add(badColumnName, new ColumnValue(3.14));
            
            var badColumnsToGet = new HashSet<string>();
            badColumnsToGet.Add(badColumnName);
            
            var errorMessage = String.Format("Invalid column name: '{0}'.", badColumnName);
            var badAttribute = new AttributeColumns();
            badAttribute.Add(badColumnName, new ColumnValue(3.14));
            
            SetTestConext(
                pkSchema:badPrimaryKeySchema, 
                primaryKey:badPrimaryKey, 
                startPrimaryKey:badPrimaryKey,
                allFailedMessage:errorMessage);
            TestAllDataAPI(deleteTable:false);
            
            SetTestConext(
                attribute:badAttribute,
                allFailedMessage:errorMessage);
            TestAllDataAPIWithAttribute(false);
            
            SetTestConext(
                columnsToGet:badColumnsToGet,
                allFailedMessage:errorMessage);
            TestAllDataAPIWithColumnsToGet();
        }
        
        // <summary>
        // 测试所有接口，列名长度为0的情况，期望返回错误消息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        // </summary>
        [Test]
        public void TestColumnNameOfZeroLength() 
        {
            testBadColumnName("");
        }

        // <summary>
        // 测试所有接口，列名包含Unicode，期望返回错误信息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        // </summary>
        [Test]
        public void TestColumnNameWithUnicode() 
        {
            testBadColumnName("中文");
        }

        // <summary>
        // 测试所有接口，列名长度为1KB，期望返回错误信息：Invalid column name: '{ColumnName}'. 中包含的ColumnName与输入一致。
        // </summary>
        [Test]
        public void Test1KBColumnName() 
        {
            testBadColumnName(new string('X', 1024));
        }
    }
}
