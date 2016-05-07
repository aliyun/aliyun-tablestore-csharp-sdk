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

namespace Aliyun.OTS.UnitTest.InterfaceTest
{
    [TestFixture]
    class TableOperationTest : OTSUnitTestBase
    {

        // <summary>
        // 创建一个表，然后DescribeTable校验TableMeta和ReservedThroughput与建表时的参数一致。
        // </summary>
        [Test]
        public void TestCreateTable() {}

        // <summary>
        // 在没有表的情况下 ListTable，期望返回0个Table Name。
        // </summary>
        [Test]
        public void TestListTableWith0Table() { }

        // <summary>
        // 在有1个表的情况下 ListTable，期望返回1个Table Name。
        // </summary>
        [Test]
        public void TestListTableWith1Table() { }

        // <summary>
        // 在有2个表的情况下 ListTable，期望返回2个Table Name。
        // </summary>
        [Test]
        public void TestListTableWith2Tables() { }

        // <summary>
        // 创建一个表，并删除，ListTable期望返回0个TableName。
        // </summary>
        [Test]
        public void TestDeleteTable() { }

        // <summary>
        // 创建一个表，CU为（10，20），UpdateTable指定CU为（5，30），DescribeTable期望返回CU为(5, 30)。
        // </summary>
        [Test]
        public void TestUpdateTable() {  }

    }
}
