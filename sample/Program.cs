using System;

namespace Aliyun.OTS.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Aliyun Table Store SDK for .NET Samples!");

            try
            {
                ClientInitialize.InitializeClient();

                CreateTableSample.TableOperations();

                //SingleRowReadWriteSample.PutRow();

                //SingleRowReadWriteSample.UpdateRow();

                //SingleRowReadWriteSample.GetRow();
                //SingleRowReadWriteSample.GetRowWithFilter();

                //MultiRowReadWriteSample.BatchWriteRow();

                //MultiRowReadWriteSample.GetRange();
                //MultiRowReadWriteSample.GetRangeWithFilter();
                //MultiRowReadWriteSample.GetIterator();

                //MultiRowReadWriteSample.BatchGetRow();
                //MultiRowReadWriteSample.BatchGetRowWithFilter();

                //ConditionUpdateSample.ConditionPutRow();
                //ConditionUpdateSample.ConditionUpdateRow();
                //ConditionUpdateSample.ConditionDeleteRow();
                //ConditionUpdateSample.ConditionBatchWriteRow();
            }
            catch (OTSException ex)
            {
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error info: {0}", ex.Message);
            }

            Console.WriteLine("Press any key to continue . . . ");
            Console.ReadKey(true);
        }
    }
}
