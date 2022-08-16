using Aliyun.OTS.DataModel.Search;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.Request
{
    public class ParallelScanRequest : OTSRequest
    {
        public string TableName { get; set; }

        public string IndexName { get; set; }
        /// <summary>
        /// 查询条件
        /// </summary>
        public ScanQuery ScanQuery { get; set; }
        /// <summary>
        /// 指定需要返回的列，<b>推荐只返回需要的列</b>
        /// </summary>
        public ColumnsToGet ColumnToGet { get; set; }
        /// <summary>
        /// 从<see cref="ComputeSplitsResponse"/>获取。
        /// </summary>
        public byte[] SessionId { get; set; }
        /// <summary>
        /// 请求级别的并发扫描超时时间，单位ms。
        /// </summary>
        public int TimeoutInMillisecond { get; set; }

        public ParallelScanRequest()
        {
            this.TimeoutInMillisecond = -1;
        }

        public ParallelScanRequest(string tableName, string indexName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
            this.TimeoutInMillisecond = -1;
        }
    }
}
