using System;
namespace Aliyun.OTS.DataModel
{
    public class TableOptions
    {
        /// <summary>
        /// 本张表中保存的数据的存活时间，单位秒
        /// </summary>
        public int? TimeToLive { get; set; }

        /// <summary>
        /// 本张表保留的最大版本数
        /// </summary>
        public int? MaxVersions { get; set; }

        /// <summary>
        /// 最大版本偏差，目的主要是为了禁止写入与预期较大的数据
        /// </summary>
        public Int64? DeviationCellVersionInSec { get; set; }

        public BloomFilterType? BloomFilterType { get; set; } // 可以动态更改

        /// <summary>
        /// 最大版本偏差，目的主要是为了禁止写入与预期较大的数据
        /// </summary>
        public int? BlockSize { get; set; }

        /// <summary>
        /// 是否允许有Update操作
        /// </summary>
        public bool? AllowUpdate { get; set; }
    }
}
