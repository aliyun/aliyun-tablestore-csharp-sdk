namespace Aliyun.OTS.Request
{
    public class DescribeSearchIndexRequest : OTSRequest
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }

        public DescribeSearchIndexRequest(string tableName, string indexName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
        }
    }
}
