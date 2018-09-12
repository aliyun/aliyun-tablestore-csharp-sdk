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

using NUnit.Framework;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;

namespace Aliyun.OTS.UnitTest
{

    [TestFixture]
    class RowOperationTest : OTSUnitTestBase
    {   
        [Test]
        public void TestInteger()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(12345) }
            };
            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);

            DeleteTable();
        }
        
        [Test]
        public void TestString()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue("abcdefghijklnm") }
            };

            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);
            DeleteTable();
        }
                
        [Test]
        public void TestDouble()
        {            
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(123.321) }
            };

            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);

            DeleteTable();
        }
        
        [Test]
        public void TestBoolean()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(true) }
            };
            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);

            DeleteTable();
        }
        
        [Test]
        public void TestBinary()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };

            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);

            DeleteTable();
        }
        
        [Test]
        public void TestColumnsToGet()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(123) },
                { "Col1", new ColumnValue("ABC") },
                { "Col2", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };

            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey,
                new HashSet<string>() {"Col0", "Col2"}
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);


            var attributeToExpect = new AttributeColumns
            {
                { "Col0", new ColumnValue(123) },
                { "Col2", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };

            AssertColumns(attributeToExpect, response2.Attribute);

            DeleteTable();
        }
        
        [Test]
        public void TestConditionIgnore()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };

            {
                var request1 = new PutRowRequest(
                    TestTableName,
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    TestTableName,
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
                    TestTableName,
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    TestTableName,
                    primaryKey
                );

                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }

            DeleteTable();
        }
        
        [Test]
        public void TestConditionExpectExist()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };
            {
                var request1 = new PutRowRequest(
                    TestTableName,
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
                    TestTableName,
                    new Condition(RowExistenceExpectation.IGNORE),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    TestTableName,
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
                    TestTableName,
                    new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    TestTableName,
                    primaryKey
                );

                var response2 = OTSClient.GetRow(request2);
                Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
                Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
                AssertColumns(primaryKey, response2.PrimaryKey);
                AssertColumns(attribute, response2.Attribute);
            }

            DeleteTable();
        }
        
        [Test]
        public void TestConditionExpectNotExist()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(new byte[] { 0x20, 0x21, 0x23, 0x24 }) }
            };

            {
                var request1 = new PutRowRequest(
                    TestTableName,
                    new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                    primaryKey,
                    attribute
                );
                
                var response1 = OTSClient.PutRow(request1);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Read);
                Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
                
                var request2 = new GetRowRequest(
                    TestTableName,
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
                    TestTableName,
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

            DeleteTable();
        }
        
        [Test]
        public void TestEmptyPrimaryKey()
        {
            CreateTable();
            var primaryKey = new PrimaryKey();

            var attribute = new AttributeColumns
            {
                { "Col0", new ColumnValue(true) }
            };

            var request1 = new PutRowRequest(
                TestTableName,
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
                Assert.AreEqual("Cell data broken, empty PK.", e.ErrorMessage);
                Assert.NotNull(e.RequestID);
            }
            
            var request2 = new GetRowRequest(
                TestTableName,
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
                Assert.AreEqual("Cell data broken, empty PK.", e.ErrorMessage);
                Assert.NotNull(e.RequestID);
            }

            DeleteTable();
        }
        
        [Test]
        public void TestEmptyAttribute()
        {
            CreateTable();

            var primaryKey = new PrimaryKey
            {
                { "PK0", new ColumnValue("ABC") },
                { "PK1", new ColumnValue(123) }
            };

            var attribute = new AttributeColumns();
            var request1 = new PutRowRequest(
                TestTableName,
                new Condition(RowExistenceExpectation.IGNORE),
                primaryKey,
                attribute
            );
            
            var response1 = OTSClient.PutRow(request1);
            Assert.AreEqual(0, response1.ConsumedCapacityUnit.Read);
            Assert.AreEqual(1, response1.ConsumedCapacityUnit.Write);
            
            var request2 = new GetRowRequest(
                TestTableName,
                primaryKey
            );

            var response2 = OTSClient.GetRow(request2);
            Assert.AreEqual(1, response2.ConsumedCapacityUnit.Read);
            Assert.AreEqual(0, response2.ConsumedCapacityUnit.Write);
            AssertColumns(primaryKey, response2.PrimaryKey);
            AssertColumns(attribute, response2.Attribute);

            DeleteTable();
        }
    }
}
