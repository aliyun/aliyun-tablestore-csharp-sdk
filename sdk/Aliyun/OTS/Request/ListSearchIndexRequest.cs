namespace Aliyun.OTS.Request
{
    /// <summary>
    /// 表示一个ListSearchIndex请求
    /// </summary>
    public class ListSearchIndexRequest : OTSRequest
	{
		public ListSearchIndexRequest(string tableName)
        {
            this.TableName = tableName;
        }

		public string TableName { get; set; }
	}
}
