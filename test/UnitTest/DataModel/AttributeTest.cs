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
    class AttributeTest : OTSUnitTestBase
    {
        // <summary>
        // 测试0个属性列时的情况，期望返回错误消息：The number of primary key columns must be in range: [1, 4].
        // </summary>
        [Test]
        public void TestNoColumnInAttribute() 
        {
            var expectedFailure = new Dictionary<string, string>();
            expectedFailure.Add("UpdateRow_Put", "No column specified while updating row.");
            expectedFailure.Add("UpdateRow_Delete", "No column specified while updating row.");
            expectedFailure.Add("BatchWriteRow_Update", "No attribute column specified to update row #0 in table: 'SampleTestName'.");
            
            SetTestConext(attribute:new AttributeColumns(), expectedFailure:expectedFailure);
            TestAllDataAPI();
        }

        // <summary>
        // 测试1个属性列时的情况。
        // </summary>
        [Test]
        public void TestOneColumnInAttribute() 
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(3.14));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试4个属性列时的情况。
        // </summary>
        [Test]
        public void TestFourColumnInAttribute() 
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(3.14));
            attribute.Add("Col1", new ColumnValue(3.14));
            attribute.Add("Col2", new ColumnValue(3.14));
            attribute.Add("Col3", new ColumnValue(3.14));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试1025个属性列的情况，目前一次限制最大写入1024列
        // </summary>
        [Test]
        public void TestTooMuchColumnInAttribute() 
        {
            var attribute = new AttributeColumns();
            
            for (int i = 0; i < 1025; i ++) {
                attribute.Add("Col" + i, new ColumnValue(3.14));
            }
            
            var expectedFailure = new Dictionary<string, string>();
            expectedFailure.Add("PutRow", "The number of columns from the request exceeded the limit.");
            expectedFailure.Add("UpdateRow_Put", "The number of columns from the request exceeded the limit.");
            expectedFailure.Add("UpdateRow_Delete", "The number of columns from the request exceeded the limit.");
            expectedFailure.Add("BatchWriteRow_Put", "The number of columns from the request exceeded the limit of putting row #0 in table: 'SampleTestName'.");
            expectedFailure.Add("BatchWriteRow_Update", "The number of columns from the request exceeded the limit of updating row #0 in table: 'SampleTestName'.");
            
            SetTestConext(attribute:attribute, expectedFailure:expectedFailure);
            TestAllDataAPIWithAttribute(true);
        }
    }
}
