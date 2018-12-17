using System.Collections.Generic;
using Aliyun.OTS.DataModel;

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// SearchIndex的返回结果
    /// </summary>
    public class SearchResponse : OTSResponse
    {
        /// <summary>
        /// 根据输入的Query语句进行查询，SearchIndex引擎返回的总命中数
        ///注:是查询到的实际数量，不是该Response中返回的具体的行数。行数可以由其他参数来控制，进行类似分页的操作
        /// </summary>
        public long TotalCount { get; set; }
        /// <summary>
        /// Query查询的具体返回结果列表
        /// </summary>
        public List<Row> Rows { get; set; }
        /// <summary>
        ///  是否查询成功
        /// </summary>
        public bool IsAllSuccess { get; set; }

        public byte[] NextToken { get; set; }
    }
}
