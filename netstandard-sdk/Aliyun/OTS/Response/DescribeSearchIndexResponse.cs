using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Response
{
    public class DescribeSearchIndexResponse : OTSResponse
    {
        public IndexSchema Schema { get; set; }
        public SyncStat SyncStat { get; set; }

    }
}
