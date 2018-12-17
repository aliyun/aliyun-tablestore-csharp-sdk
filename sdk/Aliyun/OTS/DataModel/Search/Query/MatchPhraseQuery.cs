using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// 类似 {@link MatchQuery} （MatchQuery 仅匹配某个词即可），但是 MatchPhraseQuery会匹配所有的短语。
    /// </summary>
    public class MatchPhraseQuery : IQuery
    {
        public string FieldName { get; set; }
        public string Text { get; set; }

        public MatchPhraseQuery(string fieldName, string text)
        {
            this.FieldName = fieldName;
            this.Text = text;
        }

        public QueryType GetQueryType()
        {
            return QueryType.QueryType_MatchPhraseQuery;
        }

        public ByteString Serialize()
        {
            return SearchQueryBuilder.BuildMatchPhraseQuery(this).ToByteString();
        }
    }
}
