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
    class CapacityUnitTest : OTSUnitTestBase
    {
        
        
        // <summary>
        // 测试GetRow / BatchGetRow / GetRange接口消耗3个读CU时返回的CU Consumed符合预期。
        // </summary>
        [Test]
        public void Test3ReadCUConsumed() 
        {
            CreateTestTableWith4PK();
            WaitForTableReady();
            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new String('X', 9 * 1024)));
            PutSingleRow(TestTableName, PrimaryKeyWith4Columns, attribute);
            
            SetTestConext(attribute:attribute, 
                          getRowConsumed:new CapacityUnit(3, 0), 
                          getRangeConsumed:new CapacityUnit(3, 0));
            
            TestSingleAPI("GetRow");
            TestSingleAPI("BatchGetRow");
            TestSingleAPI("GetRange");
        }

        // <summary>
        // 测试PutRow / BatchWriteRow / UpdateRow / DeleteRow接口消耗3个写CU时返回的CU Consumed符合预期。
        // </summary>
        [Test]
        public void Test3WriteCUConsumed() 
        {            
            var attribute = new AttributeColumns();
            attribute.Add("Col0", new ColumnValue(new String('X', 9 * 1024)));
            
            SetTestConext(attribute:attribute, 
                          getRowConsumed:new CapacityUnit(3, 0), 
                          getRangeConsumed:new CapacityUnit(3, 0),
                          putRowConsumed:new CapacityUnit(0, 3),
                          updateRowConsumed:new CapacityUnit(0, 3),
                          deleteRowConsumed:new CapacityUnit(0, 1));
            
            TestSingleAPI("CreateTable");
            WaitForTableReady();
            TestSingleAPI("DescribeTable");
            
            TestSingleAPI("PutRow");
            TestSingleAPI("GetRow");
            TestSingleAPI("GetRange");
            TestSingleAPI("BatchGetRow");
            TestSingleAPI("DeleteRow");
            
            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(1),
                attribute: attribute, 
                getRowConsumed: new CapacityUnit(3, 0), 
                getRangeConsumed: new CapacityUnit(3, 0),
                putRowConsumed: new CapacityUnit(0, 3),
                updateRowConsumed: new CapacityUnit(0, 3),
                deleteRowConsumed: new CapacityUnit(0, 1));
            
            TestSingleAPI("UpdateRow_Put");
            TestSingleAPI("UpdateRow_Delete");
            
            SetTestConext(primaryKey: GetPredefinedPrimaryKeyWith4PK(2),
                attribute: attribute, 
                getRowConsumed: new CapacityUnit(3, 0), 
                getRangeConsumed: new CapacityUnit(3, 0),
                putRowConsumed: new CapacityUnit(0, 3),
                updateRowConsumed: new CapacityUnit(0, 3),
                deleteRowConsumed: new CapacityUnit(0, 1));
            
            TestSingleAPI("BatchWriteRow_Put");
            TestSingleAPI("BatchWriteRow_Delete");
            TestSingleAPI("BatchWriteRow_Update");
            
        }

        // <summary>
        // UpdateRow 只更新 Read CU，DescribeTable 校验返回符合预期。
        // </summary>
        [Test]
        public void TestUpdateCUReadOnly() 
        {
            SetTestConext(reservedThroughput: new CapacityUnit(0, 0));
            TestSingleAPI("CreateTable");
            
            WaitForTableReady();
            TestSingleAPI("DescribeTable");
            
            WaitBeforeUpdateTable();
            SetTestConext(reservedThroughput: new CapacityUnit(read: 1));
            TestSingleAPI("UpdateTable");
            
            SetTestConext(reservedThroughput: new CapacityUnit(0, 0));
            TestSingleAPI("DescribeTable");
        }

        // <summary>
        // UpdateRow 只更新 Write CU，DescribeTable 校验返回符合预期。
        // </summary>
        [Test]
        public void TestUpdateCUWriteOnly() 
        {            
            SetTestConext(reservedThroughput: new CapacityUnit(0, 0));
            TestSingleAPI("CreateTable");
            
            WaitForTableReady();
            TestSingleAPI("DescribeTable");
            
            WaitBeforeUpdateTable();
            SetTestConext(reservedThroughput: new CapacityUnit(write: 1));
            TestSingleAPI("UpdateTable");
            
            SetTestConext(reservedThroughput: new CapacityUnit(0, 0));
            TestSingleAPI("DescribeTable");
        }

        // <summary>
        // CreateTable 时只指定read或者writeCU，期望返回服务端错误。
        // </summary>
        [Test]
        public void TestCreateTableWithInvalidCU()
        {
            SetTestConext(reservedThroughput: new CapacityUnit(read:1), 
                          allFailedMessage:"Both read and write capacity unit are required to create table.");
            TestSingleAPI("CreateTable");
            
            SetTestConext(reservedThroughput: new CapacityUnit(write:1), 
                          allFailedMessage: "Both read and write capacity unit are required to create table.");
            TestSingleAPI("CreateTable");
        }
    }
}
