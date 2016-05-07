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

namespace Aliyun.OTS.UnitTest
{
    
    [TestFixture]
    class RowOperationTest : OTSUnitTestBase
    {
        public void CreateTestTable()
        {
            var primaryKeys = new PrimaryKeySchema();
            primaryKeys.Add("PK0", ColumnValueType.String);
            primaryKeys.Add("PK1", ColumnValueType.Integer);
            
            var tableMeta = new TableMeta("SampleTableName", primaryKeys);
            var reservedThroughput = new CapacityUnit(0, 0);
            var request = new CreateTableRequest(tableMeta, reservedThroughput);
            OTSClient.CreateTable(request);
            
            WaitForTableReady();
        }
        
        [Test]
        public void TestInteger()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(12345));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
        
        [Test]
        public void TestString()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue("abcdefghijklnm"));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
                
        [Test]
        public void TestDouble()
        {            
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(123.321));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
        
        [Test]
        public void TestBoolean()
        {
            Console.WriteLine("1");
            CreateTestTable();
            Console.WriteLine("2");
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(true));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
        
        [Test]
        public void TestBinary()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
        
        [Test]
        public void TestColumnsToGet()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(123));
            attribute.Add("Col1", new ColumnValue("ABC"));
            attribute.Add("Col2", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey,
                new HashSet<string>() {"Col0", "Col2"}
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(new PrimaryKey(), response2.PrimaryKey);
            
            
            var attributeToExpect = new AttributeColumns();
            attributeToExpect.Add("Col0", new ColumnValue(123));
            attributeToExpect.Add("Col2", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            
            AssertColumns(attributeToExpect, response2.Attribute);
        }
        
        [Test]
        public void TestConditionIgnore()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
                
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    "SampleTableName",
                    primaryKey
                );
                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }
            
                        
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    "SampleTableName",
                    primaryKey
                );
                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }
            
        }
        
        [Test]
        public void TestConditionExpectExist()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
                
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    primaryKey,
                    attribute
                );
                
                try 
                {
                    OTSClient.PutRow(request1);
                    Assert.Fail();
                }
                catch (OTSServerException e)
                {
                    Assert.AreEqual("/PutRow", e.APIName);
                    Assert.AreEqual(403, (int)e.HttpStatusCode);
                    Assert.AreEqual("OTSConditionCheckFail", e.ErrorCode);
                    Assert.AreEqual("Condition check failed.", e.ErrorMessage);
                    Assert.NotNull(e.RequestID);
                }
            }
                    
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    "SampleTableName",
                    primaryKey
                );
                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }
                               
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    "SampleTableName",
                    primaryKey
                );
                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }
            
        }
        
        [Test]
        public void TestConditionExpectNotExist()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
                
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new byte[]{0x20, 0x21, 0x23, 0x24}));
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    "SampleTableName",
                    primaryKey
                );
                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }       
            {
                var request1 = new PutRowRequest(
                    "SampleTableName",
                    new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                    primaryKey,
                    attribute
                );
                
                try 
                {
                    OTSClient.PutRow(request1);
                    Assert.Fail();
                }
                catch (OTSServerException e)
                {
                    Assert.AreEqual("/PutRow", e.APIName);
                    Assert.AreEqual(403, (int)e.HttpStatusCode);
                    Assert.AreEqual("OTSConditionCheckFail", e.ErrorCode);
                    Assert.AreEqual("Condition check failed.", e.ErrorMessage);
                    Assert.NotNull(e.RequestID);
                }
            }
        }
        
        [Test]
        public void TestEmptyPrimaryKey()
        {
            var primaryKey = new PrimaryKey();
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(true));
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            try
            {
                OTSClient.PutRow(request1);
                Assert.Fail();
            }
            catch (OTSServerException e)
            {
                Assert.AreEqual("/PutRow", e.APIName);
                Assert.AreEqual(400, (int)e.HttpStatusCode);
                Assert.AreEqual("OTSParameterInvalid", e.ErrorCode);
                Assert.AreEqual("The number of primary key columns must be in range: [1, 4].", e.ErrorMessage);
                Assert.NotNull(e.RequestID);
            }
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            
            try
            {
                OTSClient.GetRow(request2);
                Assert.Fail();
            }
            catch (OTSServerException e)
            {
                Assert.AreEqual("/GetRow", e.APIName);
                Assert.AreEqual(400, (int)e.HttpStatusCode);
                Assert.AreEqual("OTSParameterInvalid", e.ErrorCode);
                Assert.AreEqual("The number of primary key columns must be in range: [1, 4].", e.ErrorMessage);
                Assert.NotNull(e.RequestID);
            }
        }
        
        [Test]
        public void TestEmptyAttribute()
        {
            CreateTestTable();
            
            var primaryKey = new PrimaryKey();
            primaryKey.Add("PK0", new ColumnValue("ABC"));
            primaryKey.Add("PK1", new ColumnValue(123));
            
            var attribute = new AttributeColumns();
            var request1 = new PutRowRequest(
                "SampleTableName",
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                "SampleTableName",
                primaryKey
            );
            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
        }
    }
}
