namespace Aliyun.OTS.Request
{
    public class DeleteSearchIndexRequest : OTSRequest
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }

        public DeleteSearchIndexRequest(string tableName,string indexName) {
            this.TableName = tableName;
            this.IndexName = indexName;
        }
    }
}
