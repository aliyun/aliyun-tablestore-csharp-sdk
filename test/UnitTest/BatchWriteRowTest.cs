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
using System.Threading.Tasks;
using System.Web;
using System.Net;
using System.Net.Http;
using System.IO;

using NUnit.Framework;

using Aliyun.OTS;
using Aliyun.OTS.Response;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class BatchWriteRowTest : OTSUnitTestBase
    {
        // 正向测试，主要测试API是否能正常工作
        [Test]
        public void TestForNormal()
        {
            var schema = new PrimaryKeySchema();
            schema.Add("pk1", ColumnValueType.String);

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));

            var attr1 = new AttributeColumns();
            attr1.Add("attr", new ColumnValue("attr_value1"));
            var attr2 = new AttributeColumns();
            attr2.Add("attr", new ColumnValue("attr_value2"));
            var attr3 = new AttributeColumns();
            attr3.Add("attr", new ColumnValue("attr_value3"));

            {
                List<PrimaryKey> primaryKeys = new List<PrimaryKey>();
                var pk1 = new PrimaryKey();
                pk1.Add("pk1", new ColumnValue("1"));

                var pk2 = new PrimaryKey();
                pk2.Add("pk1", new ColumnValue("2"));

                var pk3 = new PrimaryKey();
                pk3.Add("pk1", new ColumnValue("3"));

                primaryKeys.Add(pk1);
                primaryKeys.Add(pk2);
                primaryKeys.Add(pk3);

                {
                    var request = new BatchWriteRowRequest();
                    var change = new RowChanges();
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk1, attr1);
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk2, attr2);
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk3, attr3);

                    request.Add(TestTableName, change);

                    var response = OTSClient.BatchWriteRow(request);
                }

                {
                    var request = new BatchGetRowRequest();
                    request.Add(TestTableName, primaryKeys);

                    var response = OTSClient.BatchGetRow(request);
                    var tables = response.RowDataGroupByTable;
                    Assert.AreEqual(1, tables.Count);

                    var rows = tables[TestTableName];

                    Assert.AreEqual(3, rows.Count);

                    AssertPrimaryKey(pk1, rows[0].PrimaryKey);
                    AssertPrimaryKey(pk2, rows[1].PrimaryKey);
                    AssertPrimaryKey(pk3, rows[2].PrimaryKey);

                    AssertAttribute(attr1, rows[0].Attribute);
                    AssertAttribute(attr2, rows[1].Attribute);
                    AssertAttribute(attr3, rows[2].Attribute);
                }
                
            }
        }

        // 反向测试，主要测试API在异常
        [Test]
        public void TestForError()
        {
            var schema = new PrimaryKeySchema();
            schema.Add("pk1", ColumnValueType.String);

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));

            var attr1 = new AttributeColumns();
            attr1.Add("attr", new ColumnValue("attr_value1"));
            var attr2 = new AttributeColumns();
            attr2.Add("attr", new ColumnValue("attr_value2"));
            var attr3 = new AttributeColumns();
            attr3.Add("attr", new ColumnValue("attr_value3"));

            {
                var pk1 = new PrimaryKey();
                pk1.Add("pk1", new ColumnValue("1"));
                pk1.Add("pk2", new ColumnValue("1"));

                var pk2 = new PrimaryKey();
                pk2.Add("pk1", new ColumnValue("2"));
                pk2.Add("pk2", new ColumnValue("2"));

                var pk3 = new PrimaryKey();
                pk3.Add("pk1", new ColumnValue("3"));
                pk3.Add("pk2", new ColumnValue("3"));

                {
                    var request = new BatchWriteRowRequest();
                    var change = new RowChanges();
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk1, attr1);
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk2, attr2);
                    change.AddPut(new Condition(RowExistenceExpectation.IGNORE), pk3, attr3);

                    request.Add(TestTableName, change);

                    var response = OTSClient.BatchWriteRow(request);
                    var tables = response.TableRespones;

                    Assert.AreEqual(1, tables.Count);

                    var rows = tables[TestTableName];

                    Assert.AreEqual(3, rows.PutResponses.Count);

                    Assert.AreEqual("OTSInvalidPK", rows.PutResponses[0].ErrorCode);
                    Assert.AreEqual("OTSInvalidPK", rows.PutResponses[1].ErrorCode);
                    Assert.AreEqual("OTSInvalidPK", rows.PutResponses[2].ErrorCode);

                    Assert.AreEqual("Primary key schema mismatch.", rows.PutResponses[0].ErrorMessage);
                    Assert.AreEqual("Primary key schema mismatch.", rows.PutResponses[1].ErrorMessage);
                    Assert.AreEqual("Primary key schema mismatch.", rows.PutResponses[2].ErrorMessage);

                }
            }
        }
    }
}
