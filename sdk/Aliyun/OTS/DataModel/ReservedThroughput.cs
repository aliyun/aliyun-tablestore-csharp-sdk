namespace Aliyun.OTS.DataModel
{
    public class ReservedThroughput
    {
        /// <summary>
        /// 表的预留吞吐量配置
        /// </summary>
        public CapacityUnit CapacityUnit { get; set; }

        /// <summary>
        /// 初始化<see cref="CapacityUnit"/>，使用默认的预留读写吞吐量配置（0单位的都能力和0单位的写能力）
        /// </summary>
        public ReservedThroughput()
        {
            CapacityUnit = new CapacityUnit(0, 0);
        }

        /// <summary>
        /// 使用<paramref name="capacityUnit"/>初始化<see cref="CapacityUnit"/>
        /// </summary>
        public ReservedThroughput(CapacityUnit capacityUnit)
        {
            CapacityUnit = capacityUnit;
        }

        /// <summary>
        /// 使用<paramref name="read"/>和<paramref name="write"/>初始化<see cref="capacityUnit"/>
        /// </summary>
        public ReservedThroughput(int read, int write)
        {
            CapacityUnit = new CapacityUnit(read, write);
        }
    }
}
