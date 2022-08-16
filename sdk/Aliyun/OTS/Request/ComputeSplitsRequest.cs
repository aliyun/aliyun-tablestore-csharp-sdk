using Aliyun.OTS.DataModel;
using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Request
{
    /// <summary>
    ///  指定分片大小
    /// </summary>
    public class ComputeSplitsRequest : OTSRequest
    {
        public string TableName { get; set; }

        public ISplitsOptions SplitOptions { get; set; }

        public ComputeSplitsRequest() { }

        /// <summary>
        /// 通过表名，分片选项指定
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="splitOptions"></param>
        public ComputeSplitsRequest(string tableName, ISplitsOptions splitOptions)
        {
            TableName = tableName;
            SplitOptions = splitOptions;
        }
        /// <summary>
        /// 获取多元索引分片选项。
        /// </summary>
        /// <returns>SearchIndexSplitsOptions</returns>
        public SearchIndexSplitsOptions GetSearchIndexSplitsOptions()
        {
            return SplitOptions as SearchIndexSplitsOptions;
        }
    }
}
