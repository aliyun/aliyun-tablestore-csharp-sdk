using System.Collections.Generic;
using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Response
{
    /// <summary>
    /// 表示ListSearchIndex的返回
    /// </summary>
    public class ListSearchIndexResponse: OTSResponse
	{
		public ListSearchIndexResponse() { }

		public List<SearchIndexInfo> IndexInfos { get; set; }
	}
}
