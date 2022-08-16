namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 读写操作消耗的能力单元和数据大小。
    /// 读操作会消耗读能力单元，例如GetRow、GetRange和BatchGetRow等；
    /// 写操作会消耗写能力单元，例如PutRow、UpdateRow、DeleteRow和BatchWriteRow等。
    /// </summary>
    public class ConsumedCapacity
    {
        /// <summary>
        /// 消耗的能力单元值
        /// </summary>
        public CapacityUnit CapacityUnit { get; set; }
        /// <summary>
        /// 消耗的数据大小值
        /// </summary>
        public CapacityDataSize CapacityDataSize { get; set; }

        public ConsumedCapacity(CapacityUnit capacityUnit)
        {
            if (capacityUnit != null)
            {
                CapacityUnit = capacityUnit;
            }
        }

        public ConsumedCapacity(CapacityDataSize capacityDataSize)
        {
            if (capacityDataSize != null)
            {
                CapacityDataSize = capacityDataSize;
            }
        }

        public ConsumedCapacity(CapacityUnit capacityUnit, CapacityDataSize capacityDataSize)
        {
            if (capacityDataSize != null && capacityUnit != null)
            {
                CapacityDataSize = capacityDataSize;
                CapacityUnit = capacityUnit;
            }
        }
    }
}
