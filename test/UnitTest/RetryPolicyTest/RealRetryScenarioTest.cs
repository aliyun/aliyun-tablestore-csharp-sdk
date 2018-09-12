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

namespace Aliyun.OTS.UnitTest.RetryPolicyTest
{

    [TestFixture]
    class RealRetryScenarioTest : OTSUnitTestBase
    {
        // <summary>
        // 设置表的CU为（1，1），重复进行 GetRow 20次，期望每次都成功。
        // </summary>
        [Test]
        public void TestRetryWithOTSNotEnoughCapacityUnit() 
        {
            var schema = new PrimaryKeySchema
            {
                { "PK0", ColumnValueType.String },
                { "PK1", ColumnValueType.Integer }
            };

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("ABC") }
            };

            PutSingleRow(TestTableName, primaryKey, attribute);
            
            for (int i = 0; i < 20; i ++)
            {
                CheckSingleRow(TestTableName, primaryKey, attribute, new CapacityUnit(1, 0));
            }

            DeleteTable(TestTableName);
        }

    }
}
