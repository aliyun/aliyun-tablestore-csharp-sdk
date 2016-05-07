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
    class PrimaryKeyTest : OTSUnitTestBase
    {
        // <summary>
        // 测试包含0个PK列时的情况，期望返回错误消息：The number of primary key columns must be in range: [1, 4].
        // </summary>
        [Test]
        public void TestNoColumnInPK() 
        {
            var primaryKeySchema = new PrimaryKeySchema();
            var primaryKey = new PrimaryKey();
            
            SetTestConext(pkSchema:primaryKeySchema, primaryKey:primaryKey,
                          endPrimaryKey:primaryKey,
                         allFailedMessage:"The number of primary key columns must be in range: [1, 4].");
            
            TestSingleAPI("CreateTable");
            CreateTestTableWith4PK();
            
            TestAllDataAPI(createTable:false);
        }

        // <summary>
        // 测试1个PK列时的情况。
        // </summary>
        [Test]
        public void TestOneColumnInPK() 
        {
            var primaryKeySchema = new PrimaryKeySchema();
            primaryKeySchema.Add("PK0", ColumnValueType.Integer);
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue(123));
            
            var startPrimaryKey = new PrimaryKey();
            startPrimaryKey.Add("PK0", ColumnValue.INF_MIN);

            var endPrimaryKey = new PrimaryKey();
            endPrimaryKey.Add("PK0", ColumnValue.INF_MAX);
            
            SetTestConext(pkSchema:primaryKeySchema, primaryKey:primaryKey,
                          startPrimaryKey:startPrimaryKey, endPrimaryKey:endPrimaryKey);
            
            TestSingleAPI("CreateTable");
            TestSingleAPI("DescribeTable");
            WaitForTableReady();
            TestAllDataAPI(createTable:false);
        }

        // <summary>
        // 测试4个PK列时的情况。
        // </summary>
        [Test]
        public void TestFourColumnInPK() 
        {
            // 这里大量case测试都是4个PK的情况。。。不比再重复一次了吧
        }

        // <summary>
        // 测试1000个PK列的情况，期望返回错误消息：The number of primary key columns must be in range: [1, 4].
        // </summary>
        [Test]
        public void TestTooMuchColumnInPK() 
        {
            var primaryKeySchema = new PrimaryKeySchema();
            var primaryKey = new PrimaryKey();
            
            for (int i = 0; i < 1000; i ++) {
                primaryKeySchema.Add("PK" + i, ColumnValueType.Integer);
                primaryKey.Add("PK" + i, new ColumnValue(123));
            }
            
            SetTestConext(pkSchema:primaryKeySchema, primaryKey:primaryKey,
                          endPrimaryKey:primaryKey,
                         allFailedMessage:"The number of primary key columns must be in range: [1, 4].");
            
            TestSingleAPI("CreateTable");
            CreateTestTableWith4PK();
            
            TestAllDataAPI(createTable:false);
        }
    }
}
