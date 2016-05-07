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
    class ColumnValueTest : OTSUnitTestBase
    {
        // <summary>
        // 测试StringValue为10个字节的情况。
        // </summary>
        [Test]
        public void TestNormalStringValue() 
        {
            var targetString = new string('X', 10);
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue(targetString));
            primaryKey.Add("PK1", new ColumnValue("ABC"));
            primaryKey.Add("PK2", new ColumnValue(123));
            primaryKey.Add("PK3", new ColumnValue(456));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(targetString));
            
            SetTestConext(primaryKey:primaryKey, attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试StringValue包含Unicode字符的情况。
        // </summary>
        [Test]
        public void TestUnicodeStringValue() 
        {
            string targetString = "中文字符";
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue(targetString));
            primaryKey.Add("PK1", new ColumnValue("ABC"));
            primaryKey.Add("PK2", new ColumnValue(123));
            primaryKey.Add("PK3", new ColumnValue(456));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(targetString));
            
            SetTestConext(primaryKey:primaryKey, attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试空字符串的情况。
        // </summary>
        [Test]
        public void TestEmptyStringValue() 
        {
            string targetString = "";
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue(targetString));
            primaryKey.Add("PK1", new ColumnValue("ABC"));
            primaryKey.Add("PK2", new ColumnValue(123));
            primaryKey.Add("PK3", new ColumnValue(456));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(targetString));
            
            SetTestConext(primaryKey:primaryKey, attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试字符串长度为1MB的情况，期望返回错误消息。
        // </summary>
        [Test]
        public void TestStringValueTooLong() 
        {
            string targetString = new string('X', 1024 * 1024);
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue(targetString));
            primaryKey.Add("PK1", new ColumnValue("ABC"));
            primaryKey.Add("PK2", new ColumnValue(123));
            primaryKey.Add("PK3", new ColumnValue(456));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(targetString));
            
            SetTestConext(
                primaryKey:primaryKey, 
                attribute:attribute,
                startPrimaryKey:primaryKey,
                endPrimaryKey:primaryKey,
                allFailedMessage:"The length of primary key column: 'PK0' exceeded the MaxLength:1024 with CurrentLength:1048576.");
            TestAllDataAPI(false);
        }

        byte[] generateBinaryString(long length)
        {
            byte[] ret = new byte[length];
            
            for (long i = 0; i < length; i ++)
            {
                ret[i] = (byte)(i & 0xFF);
            }
            
            return ret;
        }
        
        // <summary>
        // 测试BinaryValue为10个字节的情况。
        // </summary>
        [Test]
        public void TestNormalBinaryValue() 
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(generateBinaryString(10)));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试BinaryValue为空的情况。
        // </summary>
        [Test]
        public void TestEmptyBinaryValue() 
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(generateBinaryString(0)));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }
        
        // <summary>
        // 测试BinaryValue为1MB的情况，期望返回错误消息。
        // </summary>
        [Test]
        public void TestBinaryValueTooLong() 
        {
            CreateTestTableWith4PK();
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(generateBinaryString(1024 * 1024)));
            
            SetTestConext(attribute:attribute,
               allFailedMessage:"The length of attribute column: 'Col0' exceeded the MaxLength:65536 with CurrentLength:1048576.");
            TestAllDataAPIWithColumnValue(false);
        }

        // <summary>
        // 测试IntegerValue值为10的情况。
        // </summary>
        [Test]
        public void TestNormalIntegerValue() 
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue("DEF"));
            primaryKey.Add("PK2", new ColumnValue(10));
            primaryKey.Add("PK3", new ColumnValue(456));
            SetTestConext(primaryKey:primaryKey);
            TestAllDataAPI();
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(10));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试IntegerValue的值为8位有符号整数的最小值或最大值的情况
        // </summary>
        [Test]
        public void TestIntegerValueInBoundary() 
        {
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue("DEF"));
            primaryKey.Add("PK2", new ColumnValue(Int64.MaxValue));
            primaryKey.Add("PK3", new ColumnValue(456));
            SetTestConext(primaryKey:primaryKey);
            TestAllDataAPI();
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(Int64.MaxValue));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
            
            primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue("DEF"));
            primaryKey.Add("PK2", new ColumnValue(Int64.MinValue));
            primaryKey.Add("PK3", new ColumnValue(456));
            SetTestConext(primaryKey:primaryKey);
            TestAllDataAPI();
            
            attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(Int64.MinValue));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试IntegerValue的值为8位有符号整数的最小值-1，或者最大值+1的情况（某些SDK无法进行这样的赋值，则认为该CASE自动PASS），期望抛出ClientError。
        // </summary>
        [Test]
        public void TestIntegerValueOutOfBoundary() 
        {
            // Int64.MaxValue + 1 和  Int64.MinValue - 1 都会导致编译时溢出
        }

        // <summary>
        // 测试DoubleValue值为3.1415926的情况。
        // </summary>
        [Test]
        public void TestNormalDoubleValue()
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(3.1415926));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
            
            attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(3.1415926));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试DoubleValue的值为8位有符号浮点数的最小值或最大值的情况
        // </summary>
        [Test]
        public void TestDoubleValueInBoundary() 
        {            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(double.MaxValue));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
            
            attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(double.MinValue));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试DoubleValue的值为8位有符号浮点数的超出最小值，或者超出最大值的情况（某些SDK无法进行这样的赋值，则认为该CASE自动PASS），期望抛出ClientError。
        // </summary>
        [Test]
        public void TestDoubleValueOutOfBoundary() 
        {
            CreateTestTableWith4PK();
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(double.MaxValue * 1.1));
            SetTestConext(attribute:attribute, allFailedMessage:"The input parameter is invalid.");
            TestAllDataAPIWithColumnValue(createTable:false);
            
            attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(double.MinValue * 1.1));
            SetTestConext(attribute:attribute, allFailedMessage:"The input parameter is invalid.");
            TestAllDataAPIWithColumnValue(createTable:false);
        }

        // <summary>
        // 测试布尔值为True的情况。
        // </summary>
        [Test]
        public void TestBooleanValueTrue() 
        {
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(true));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试布尔值为False的情况。
        // </summary>
        [Test]
        public void TestBooleanValueFalse() 
        {            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(false));
            SetTestConext(attribute:attribute);
            TestAllDataAPI();
        }

        // <summary>
        // 测试值类型非法（为INF_MAX或者INF_MIN）的情况，期望抛出ClientError。某些SDK无法进行这样的赋值，则认为该CASE自动PASS。
        // </summary>
        [Test]
        public void TestInvalidValueType() 
        {            
            CreateTestTableWith4PK();
            var attribute = new AttributeColumns();
            attribute.Add("Col0", ColumnValue.INF_MAX);
            SetTestConext(attribute:attribute, allFailedMessage:"INF_MAX is an invalid type for the attribute column.");
            TestAllDataAPIWithColumnValue(createTable:false);
            
            attribute = new AttributeColumns();
            attribute.Add("Col0", ColumnValue.INF_MIN);
            SetTestConext(attribute:attribute, allFailedMessage:"INF_MIN is an invalid type for the attribute column.");
            TestAllDataAPIWithColumnValue(createTable:false);
        }

        // <summary>
        // 测试Column类型与值类型不一致的情况，期望抛出ClientError。某些SDK无法进行这样的赋值，则认为该CASE自动PASS。
        // </summary>
        [Test]
        public void TestColumnTypeAndValueMismatch() 
        {
            // C# SDK 无法构造ColumnValue值和类型不一致
        }

    }
}
