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


namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// CapacityUnit（读写能力单元）是OTS用来计量的单位。
    /// <para>
    /// 请参见：<a href="http://docs.aliyun.com/ots/DeveloperGuide/ots-table">阿里云官网关于读写能力单元的描述</a>。
    /// </para>
    /// </summary>
    public class CapacityUnit
    {
        /// <summary>
        /// 读能力单元
        /// </summary>
        public int? Read { get; set; }

        /// <summary>
        /// 写能力单元
        /// </summary>
        public int? Write { get; set; }

        /// <summary>
        /// CapacityUnit的构造函数。
        /// <para>
        /// 当UpdateRow指定预留读写吞吐量时，读和写预留吞吐量可以指定其中一个，表示只调节一个值；也可以两个都指定。
        /// 其他情况下构造CapacityUnit必须同时指定read和write参数。
        /// </para>
        /// </summary>
        /// <param name="read">读能力单元</param>
        /// <param name="write">写能力单元</param>
        public CapacityUnit(int? read = null, int? write = null)
        {
            if (read.HasValue)
            {
                Read = read;
            }

            if (write.HasValue)
            {
                Write = write;
            }
        }
    }
}
