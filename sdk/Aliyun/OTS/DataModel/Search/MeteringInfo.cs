namespace Aliyun.OTS.DataModel.Search
{
    public class MeteringInfo
    {
        /// <summary>
        /// 索引预留吞吐量
        /// </summary> 
        public ReservedThroughput ReservedThroughput { set; get; }

        /// <summary>
        /// 索引表的存储大小，该大小为上一次计量时（通过timestamp获取计量时间）统计得到的值，并非当前时刻的值
        /// </summary>
        public long StorageSize { set; get; }

        /// <summary>
        /// 索引表的总行数，该行数为上一次计量时（通过timestamp获取计量时间）统计得到的值，并非当前时刻的值
        /// </summary>
        public long RowCount { set; get; }

        /// <summary>
        /// 索引表上一次计量的时间
        /// </summary>
        public long Timestamp { set; get; }

    }
}
