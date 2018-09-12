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
using Aliyun.OTS.Util;
namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class OtsUtilsTest
    {
        [Test]
        public void TestFormatDateTimeStr()
        {
            var dt = DateTime.Parse("2018-04-26T05:12:30");
            var dateStr = OtsUtils.FormatDateTimeStr(dt);
            Assert.AreEqual("2018-04-26T05:12:30.000Z", dateStr);
        }
    }
}
