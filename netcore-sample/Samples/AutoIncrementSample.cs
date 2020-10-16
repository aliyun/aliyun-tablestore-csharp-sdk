using System;
using System.Collections.Generic;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.Samples.Samples
{
    /// <summary>
    /// 主键列自增示例
    /// </summary>
    public class AutoIncrementSample
    {
        private static readonly string TableName = "AutoIncrementSample";

        private static readonly string Pk1 = "Pk1";
        private static readonly string Pk2 = "Pk2_AutoIncrement";


        //static void Main(string[] args)
        //{
        //    Console.WriteLine("AutoIncrementSample");

        //    //创建一个带自增列的表
        //    CreateTableWithAutoIncrementPk();

        //    //写入10行，自增列Pk2将
        //    for (int i = 0; i < 10; i++)
        //    {  
        //        PutRow(i.ToString());
        //    }

        //    Console.ReadLine();

        //}

        /// <summary>
        /// 创建一个带自增列的表
        /// </summary>
        private static void CreateTableWithAutoIncrementPk()
        {
            
            OTSClient otsClient = Config.GetClient();

            IList<string> tables = otsClient.ListTable(new ListTableRequest()).TableNames;
            if (tables.Contains(TableName))
            {
                return;
            }
            
            PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema
            {
                { Pk1, ColumnValueType.String },
                //指定Pk2为自增列主键
                { Pk2, ColumnValueType.Integer, PrimaryKeyOption.AUTO_INCREMENT}
            };
            TableMeta tableMeta = new TableMeta(TableName, primaryKeySchema);

            CapacityUnit reservedThroughput = new CapacityUnit(0, 0);
            CreateTableRequest request = new CreateTableRequest(tableMeta, reservedThroughput);
            otsClient.CreateTable(request);

        }

        public static void PutRow(string pk1Value)
        {
            Console.WriteLine("Start put row...");
            OTSClient otsClient = Config.GetClient();

            // 定义行的主键，必须与创建表时的TableMeta中定义的一致
            PrimaryKey primaryKey = new PrimaryKey
            {
                { Pk1, new ColumnValue(pk1Value) },
                { Pk2,  ColumnValue.AUTO_INCREMENT }
            };

            // 定义要写入该行的属性列
            AttributeColumns attribute = new AttributeColumns
            {
                { "Col1", new ColumnValue(0) }
            };
            PutRowRequest request = new PutRowRequest(TableName, new Condition(RowExistenceExpectation.IGNORE), primaryKey, attribute);
            request.RowPutChange.ReturnType = ReturnType.RT_PK;

            var response =  otsClient.PutRow(request);
            Console.WriteLine("Put row succeed，autoIncrement Pk value:"+ response.Row.GetPrimaryKey()[Pk2].IntegerValue);
        }
        
    }
}
