namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 表的读写吞吐量的单位，即能力单元
    /// 主要用于<see cref="ReservedThroughput"/>中配置表的预留读写吞吐量以及标识读写操作消耗的能力单元的值。
    /// </summary>
    public class CapacityDataSize
    {
        /// <summary>
        /// 读能力单元
        /// </summary>
        public long? ReadCapacityDataSize { get; set; }
        /// <summary>
        /// 写能力单元
        /// </summary>
        public long? WriteCapacityDataSize { get; set; }

        public CapacityDataSize()
        {
        }

        public CapacityDataSize(long? readCapacityDataSize, long? writeCapacityDataSize)
        {
            if (readCapacityDataSize.HasValue)
            {
                ReadCapacityDataSize = readCapacityDataSize;
            }

            if (writeCapacityDataSize.HasValue)
            {
                ReadCapacityDataSize = writeCapacityDataSize;
            }
        }
    }
}
