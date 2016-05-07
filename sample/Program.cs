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
                //SingleRowReadWriteSample.PutRowAsync();

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
            catch (OTSClientException ex)
            {
                Console.WriteLine("Failed with client exception:{0}", ex.Message);
            }
            catch (OTSServerException ex)
            {
                Console.WriteLine("Failed with server exception:{0}, {1}", ex.Message, ex.RequestID);
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
