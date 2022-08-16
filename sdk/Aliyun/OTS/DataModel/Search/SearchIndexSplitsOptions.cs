namespace Aliyun.OTS.DataModel.Search
{
    public class SearchIndexSplitsOptions : ISplitsOptions
    {
        public string IndexName { get; set; }

        public SearchIndexSplitsOptions() { }

        public SearchIndexSplitsOptions(string indexName)
        {
            IndexName = indexName;
        }
    }
}
