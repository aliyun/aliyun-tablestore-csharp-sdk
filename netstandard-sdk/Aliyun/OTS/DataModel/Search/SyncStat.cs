namespace Aliyun.OTS.DataModel.Search
{
    public enum SyncPhase
    {
        FULL,
        INCR
    }

    public class SyncStat
    {

        public SyncPhase SyncPhase;
        public long CurrentSyncTimestamp;
    }
}
