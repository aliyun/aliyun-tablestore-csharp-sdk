using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Request
{
    public class CreateSearchIndexRequest : OTSRequest
    {
        /// <summary>
        ///  Tablestore中的表名称
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// SearchIndex的名称
        /// </summary>
        public string IndexName { get; set; }
        /// <summary>
        /// SearchIndex的Schema结构
        /// </summary>
        public IndexSchema IndexSchame { get; set; }

        /// <summary>
        /// 一般情况下，不需要设置本字段。
        /// <b>仅在动态修改多元索引Schema场景下</b>设置本字段，作为重建索引的源索引名字。
        /// </summary>
        public string SourceIndexName { get; set; }

        /// <summary>
        /// 索引数据的TTL时间，单位S。在表创建后，该配置项可通过调用<see cref="UpdateSearchIndexRequest"/>动态更改。
        /// </summary>
        public int? TimeToLive { get; set; }

        /// <summary>
        /// 初始化创建多元索引请求
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="indexName"></param>
        public CreateSearchIndexRequest(string tableName, string indexName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
        }

        /// <summary>
        /// 设置索引数据的TTL时间，单位S。
        /// </summary>
        /// <param name="timeToLive"></param>
        public void SetTimeToLive(int timeToLive)
        {
            TimeToLive = timeToLive;
        }
    }
}
