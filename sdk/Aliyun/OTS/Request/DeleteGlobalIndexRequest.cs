namespace Aliyun.OTS.Request
{
    public class DeleteGlobalIndexRequest : OTSRequest
    {
        public string MainTableName { get; set; }

        public string IndexName { get; set; }

        public DeleteGlobalIndexRequest(string mainTableName, string indexName)
        {
            this.MainTableName = mainTableName;
            this.IndexName = indexName;
        }
    }
}
