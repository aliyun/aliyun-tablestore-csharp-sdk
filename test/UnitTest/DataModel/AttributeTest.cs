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

using NUnit.Framework;
using Aliyun.OTS.DataModel;

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
            var expectedFailure = new Dictionary<string, string>
            {
                { "UpdateRow_Put", "Attribute column is missing." },
                { "UpdateRow_Delete", "Attribute column is missing." },
                { "BatchWriteRow_Update", "Invalid update row request: missing cells in request." }
            };

            SetTestConext(attribute: new AttributeColumns(), expectedFailure: expectedFailure);
            TestAllDataAPI();
        }

        // <summary>
        // 测试1个属性列时的情况。
        // </summary>
        [Test]
        public void TestOneColumnInAttribute()
        {
            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(3.14) }
            };

            SetTestConext(attribute: attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试4个属性列时的情况。
        // </summary>
        [Test]
        public void TestFourColumnInAttribute() 
        {
            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(3.14) },
                { "Col1", new ColumnValue(3.14) },
                { "Col2", new ColumnValue(3.14) },
                { "Col3", new ColumnValue(3.14) }
            };
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

            var expectedFailure = new Dictionary<string, string>
            {
                { "PutRow", "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025." },
                { "UpdateRow_Put", "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025." },
                { "UpdateRow_Delete", "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025." },
                { "BatchWriteRow_Put", "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025." },
                { "BatchWriteRow_Update", "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025." }
            };

            SetTestConext(attribute:attribute, expectedFailure:expectedFailure);
            TestAllDataAPIWithAttribute(true);
        }
    }
}
