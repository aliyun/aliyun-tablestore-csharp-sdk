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
            var schema = new PrimaryKeySchema();
            schema.Add("PK0", ColumnValueType.String);
            schema.Add("PK1", ColumnValueType.Integer);
            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue("ABC"));
            
            PutSingleRow(TestTableName, primaryKey, attribute);
            
            for (int i = 0; i < 20; i ++)
            {
                CheckSingleRow(TestTableName, primaryKey, attribute, new CapacityUnit(1, 0));
            }
        }

    }
}
