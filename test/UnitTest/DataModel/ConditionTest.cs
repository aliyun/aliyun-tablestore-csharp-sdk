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

namespace Aliyun.OTS.UnitTest.DataModel
{
    [TestFixture]
    class ConditionTest : OTSUnitTestBase
    {
        // <summary>
        // 测试行不存在的条件下，写操作的Condition为IGNORE，期望操作成功。
        // </summary>
        [Test]
        public void TestIgnoreConditionWhenRowNotExist() 
        {
            CreateTestTableWith4PK();
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(0), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");

            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(1),
                          condition: new Condition(RowExistenceExpectation.IGNORE),
                          deleteRowConsumed: new CapacityUnit(0, 1));
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("UpdateRow_Delete");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(4), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("GetRow"); 
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("BatchWriteRow_Update");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("BatchWriteRow_Delete");
        }

        // <summary>
        // 测试行存在的条件下，写操作的Condition为IGNORE，期望操作成功。
        // </summary>
        [Test]
        public void TestIgnoreConditionWhenRowExist() 
        {
            CreateTestTableWith4PK();
            for (int i = 0; i < 7; i ++) {
                PutSinglePredefinedRow(i);
            }
                
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(0), 
                          condition:new Condition(RowExistenceExpectation.IGNORE),
                          putRowConsumed: new CapacityUnit(0, 1));
            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(1), 
                          condition:new Condition(RowExistenceExpectation.IGNORE),
                          deleteRowConsumed: new CapacityUnit(0, 1));
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("UpdateRow_Delete");

            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(4), condition: new Condition(RowExistenceExpectation.IGNORE),
                         putRowConsumed: new CapacityUnit(0, 1));
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("GetRow"); 
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("BatchWriteRow_Update");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition:new Condition(RowExistenceExpectation.IGNORE));
            TestSingleAPI("BatchWriteRow_Delete"); 
        }

        // <summary>
        // 测试行不存在的条件下，写操作的Condition为EXPECT_EXIST，期望服务端返回 Invalid Condition。
        // </summary>
        [Test]
        public void TestExpectExistConditionWhenRowNotExist() 
        {
            CreateTestTableWith4PK();
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(0), condition:new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("PutRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(1), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("UpdateRow_Put");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("UpdateRow_Delete");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(4), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Put");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Update");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Delete");
        }

        // <summary>
        // 测试行存在的条件下，写操作的Condition为EXPECT_EXIST，期望操作成功。
        // </summary>
        [Test]
        public void TestExpectExistConditionWhenRowExist() 
        {
            CreateTestTableWith4PK();
            for (int i = 0; i < 7; i ++) {
                PutSinglePredefinedRow(i);
            }

            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(0), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                          putRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");

            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(1), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    deleteRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    updateRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    deleteRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("UpdateRow_Delete");

            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(4), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                         putRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("GetRow"); 
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    updateRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("BatchWriteRow_Update");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition: new Condition(RowExistenceExpectation.EXPECT_EXIST),
                    deleteRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("BatchWriteRow_Delete"); 
        }

        // <summary>
        // 测试行不存在的条件下，写操作的Condition为EXPECT_NOT_EXIST，期望PutRow或者BatchWriteRow 的 put 操作成功，其他操作返回 Condition Check Failed。
        // </summary>
        [Test]
        public void TestExpectNotExistConditionWhenRowNotExist() 
        {
            CreateTestTableWith4PK();
                
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(0), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                    putRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(1), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            TestSingleAPI("UpdateRow_Put");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            TestSingleAPI("UpdateRow_Delete");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(4), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                    putRowConsumed: new CapacityUnit(1, 1));
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("GetRow"); 
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            TestSingleAPI("BatchWriteRow_Update");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            TestSingleAPI("BatchWriteRow_Delete"); 
        }

        // <summary>
        // 测试行存在的条件下，写操作的Condition为EXPECT_NOT_EXIST，期望PutRow或者BatchWriteRow 的 put 操作返回Invalid Condition，其他操作返回 Condition Check Failed。
        // </summary>
        [Test]
        public void TestExpectNotExistConditionWhenRowExist() 
        {
            CreateTestTableWith4PK();
            for (int i = 0; i < 7; i ++) {
                PutSinglePredefinedRow(i);
            }
                
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(0), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("PutRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(1), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                          allFailedMessage:"Condition check failed.");
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(2), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                          allFailedMessage:"Condition check failed.");
            TestSingleAPI("UpdateRow_Put");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(3), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                          allFailedMessage:"Condition check failed.");
            TestSingleAPI("UpdateRow_Delete");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(4), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                         allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Put");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(5), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                          allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Update");
            
            SetTestConext(primaryKey:GetPredefinedPrimaryKeyWith4PK(6), condition: new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
                          allFailedMessage:"Condition check failed.");
            TestSingleAPI("BatchWriteRow_Delete"); 
        }

    }
}
