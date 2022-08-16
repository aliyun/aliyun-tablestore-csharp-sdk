using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.Response;

namespace Aliyun.OTS.DataModel.Search
{
    public class ScanQuery
    {
        public IQuery Query { get; set; }

        /// <summary>
        /// 一次网络请求中返回数据量的限制
        /// </summary>
        public int? Limit { get; set; }
        /// <summary>
        /// 最大并发数，默认为1；该值可以参考<see cref="ComputeSplitsResponse"/>的取值，并且不可大于该值。
        /// </summary>
        public int? MaxParallel { get; set; }
        /// <summary>
        /// 当前的并发ID，取值范围为：[0, maxParallel]
        /// </summary>
        public int? CurrentParallelId { get; set; }
        /// <summary>
        /// 该请求的存活时间，单位秒(s)，默认为60s。超时后请求需要重新初始化，当每次数据返回后，计时器重新刷新。
        /// </summary>
        public int? AliveTime { get; set; }
        /// <summary>
        /// 用于翻页。
        /// </summary>
        public byte[] Token { get; set; }

        public ScanQuery() { }

        public ScanQuery(IQuery query)
        {
            Query = query;
        }

        public void SetToken(byte[] token)
        {
            Token = token;
        }
    }
}
