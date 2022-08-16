using Aliyun.OTS.DataModel.Search;

namespace Aliyun.OTS.Response
{
    public class DescribeSearchIndexResponse : OTSResponse
    {
        public IndexSchema Schema { get; set; }

        public SyncStat SyncStat { get; set; }

        public MeteringInfo MeteringInfo { get; set; }

        public string BrotherIndexName { get; set; }

        public long? CreateTime { get; set; }

        public int? TimeToLive { get; set; }
    }
}
