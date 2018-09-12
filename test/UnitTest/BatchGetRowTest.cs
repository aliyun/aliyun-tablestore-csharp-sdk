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

using System.Collections.Generic;

using NUnit.Framework;
using Aliyun.OTS.Request;
using Aliyun.OTS.DataModel;
using System;

namespace Aliyun.OTS.UnitTest
{
    [TestFixture]
    class BatchGetRowTest : OTSUnitTestBase
    {
        const int batchCount = 4;

        // 正向测试，主要测试API是否能正常工作
        [Test]
        public void TestForNormal()
        {
            var result = PutRows();
            var primaryKeys = result.Item1;
            var columns = result.Item2;

            var batchGetRequest = new BatchGetRowRequest();

            batchGetRequest.Add(TestTableName, primaryKeys);

            var response = OTSClient.BatchGetRow(batchGetRequest);
            var tables = response.RowDataGroupByTable;
            Assert.AreEqual(1, tables.Count);

            var rows = tables[TestTableName];

            Assert.AreEqual(batchCount, rows.Count);

            for (int i = 0; i < batchCount; i++)
            {
                AssertPrimaryKey(primaryKeys[i], rows[i].Row.GetPrimaryKey());
                AssertAttribute(columns[i], (rows[i].Row as Row).GetColumns());
            }

            DeleteTable(TestTableName);
        }

        // 反向测试，主要测试API在异常
        [Test]
        public void TestForError()
        {
            var schema = new PrimaryKeySchema
            {
                { "pk1", ColumnValueType.String }
            };

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));


            var request = new BatchGetRowRequest();
            List<PrimaryKey> primaryKeys = new List<PrimaryKey>();
            var pk1 = new PrimaryKey
                {
                    { "pk1", new ColumnValue("1") },
                    { "pk2", new ColumnValue("1") }
                };

            var pk2 = new PrimaryKey
                {
                    { "pk1", new ColumnValue("2") },
                    { "pk2", new ColumnValue("2") }
                };

            var pk3 = new PrimaryKey
                {
                    { "pk1", new ColumnValue("3") },
                    { "pk2", new ColumnValue("3") }
                };

            primaryKeys.Add(pk1);
            primaryKeys.Add(pk2);
            primaryKeys.Add(pk3);

            request.Add(TestTableName, primaryKeys);

            var response = OTSClient.BatchGetRow(request);

            var tables = response.RowDataGroupByTable;
            Assert.AreEqual(1, tables.Count);

            var rows = tables[TestTableName];

            Assert.AreEqual(3, rows.Count);

            Assert.AreEqual("OTSInvalidPK", rows[0].ErrorCode);
            Assert.AreEqual("OTSInvalidPK", rows[1].ErrorCode);
            Assert.AreEqual("OTSInvalidPK", rows[2].ErrorCode);

            Assert.AreEqual("Validate PK size fail. Input: 2, Meta: 1.", rows[0].ErrorMessage);
            Assert.AreEqual("Validate PK size fail. Input: 2, Meta: 1.", rows[1].ErrorMessage);
            Assert.AreEqual("Validate PK size fail. Input: 2, Meta: 1.", rows[2].ErrorMessage);

            DeleteTable(TestTableName);
        }

        private Tuple<List<PrimaryKey>, List<AttributeColumns>> PutRows()
        {
            List<PrimaryKey> primaryKeys = new List<PrimaryKey>();
            List<AttributeColumns> columns = new List<AttributeColumns>();
            var schema = new PrimaryKeySchema
            {
                { "pk1", ColumnValueType.String }
            };

            CreateTestTable(TestTableName, schema, new CapacityUnit(0, 0));

            for (int i = 0; i < batchCount; i++)
            {
                var pk = new PrimaryKey
                {
                    { "pk1", new ColumnValue(i.ToString()) }
                };

                primaryKeys.Add(pk);

                var attr = new AttributeColumns
                {
                    { "attr", new ColumnValue("attr_value" + i) }
                };

                columns.Add(attr);
            }

            for (int i = 0; i < primaryKeys.Count; i++)
            {
                var putRequest = new PutRowRequest(TestTableName, new Condition(RowExistenceExpectation.IGNORE));
                putRequest.RowPutChange.PrimaryKey = primaryKeys[i];
                putRequest.RowPutChange.AddColumns(columns[i]);
                OTSClient.PutRow(putRequest);
            }

            return new Tuple<List<PrimaryKey>, List<AttributeColumns>>(primaryKeys, columns);
        }
    }
}
