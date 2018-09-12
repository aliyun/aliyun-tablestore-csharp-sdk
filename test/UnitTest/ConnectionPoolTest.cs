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

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    public class ConnectionPoolTest
    {
        // <summary>
        // 串行地执行1000次PutRow或GetRow请求。
        // </summary>
        [Test]
        public void TestSerialConnections() { }

        // <summary>
        // 起50（待确定）个线程并行地执行PutRow或者GetRow请求。
        // </summary>
        [Test]
        public void TestParallelConnections() { }

        // <summary>
        // 将连接池大小设为5，起50（待确定）个线程并行地执行PutRow或者GetRow请求，期望操作成功，并且校验总连接个数不超过5。
        // </summary>
        [Test]
        public void TestConnectionPoolWaiting() { }

    }
}
