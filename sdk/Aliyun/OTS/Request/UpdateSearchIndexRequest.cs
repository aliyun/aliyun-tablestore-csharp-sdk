namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 更新索引请求，目前支持更新索引的TTL。
    /// </summary>
    public class UpdateSearchIndexRequest : OTSRequest
    {
        public string TableName { get; set; }

        public string Indexname { get; set; }
        /// <summary>
        /// 索引的TTL，单位S
        /// </summary>
        public int? TimeToLive { get; private set; }

        public UpdateSearchIndexRequest() { }

        public UpdateSearchIndexRequest(string tableName, string indexName)
        {
            TableName = tableName;
            Indexname = indexName;
        }

        public void SetTimeToLive(int timeToLive)
        {
            TimeToLive = timeToLive;
        }
    }
}
