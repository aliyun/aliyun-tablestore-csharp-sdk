using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Request
{
    public class CreateSearchIndexRequest : OTSRequest
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }
        public IndexSchema IndexSchame { get; set; }

        public CreateSearchIndexRequest(string tableName, string indexName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
        }
    }
}
