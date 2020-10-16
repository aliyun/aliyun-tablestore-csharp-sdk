/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// <see cref="OTSClient.UpdateTable"/>和<see cref="OTSClient.DescribeTable"/>的返回中包含的
    /// 预留读写吞吐量详细信息。
    /// </summary>
    public class ReservedThroughputDetails
    {
        /// <summary>
        /// 读写能力单元
        /// </summary>
        public CapacityUnit CapacityUnit { get; private set; }

        /// <summary>
        /// 最后一次上调的时间
        /// </summary>
        public Int64 LastIncreaseTime { get; private set; }

        /// <summary>
        /// 最后一次下调的时间
        /// </summary>
        public Int64 LastDecreaseTime { get; private set; }

        /// <summary>
        /// 本日预留读写吞吐量下调的次数
        /// </summary>
        public int NumberOfDecreasesToday { get; private set; }

        public ReservedThroughputDetails(CapacityUnit capacityUnit,
                                         Int64 lastIncreaseTime,
                                         Int64 lastDecreaseTime,
                                         int numberOfDecreasesToday) :
                                         this(capacityUnit,
                                              lastIncreaseTime,
                                              lastDecreaseTime)
        {
            NumberOfDecreasesToday = numberOfDecreasesToday;
        }

        public ReservedThroughputDetails(CapacityUnit capacityUnit,
            Int64 lastIncreaseTime, Int64 lastDecreaseTime)
        {
            CapacityUnit = capacityUnit;
            LastIncreaseTime = lastIncreaseTime;
            LastDecreaseTime = lastDecreaseTime;
        }
    }
}
