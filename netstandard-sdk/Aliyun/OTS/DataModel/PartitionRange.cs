using System;
namespace Aliyun.OTS.DataModel
{
    public class PartitionRange
    {
        /// <summary>
        /// 范围的起始值
        /// </summary>
        /// <value>The begin.</value>
        public ColumnValue Begin { get; set; }

        /// <summary>
        /// 范围的结束值
        /// </summary>
        /// <value>The end.</value>
        public ColumnValue End { get; set; }

        public PartitionRange(ColumnValue begin, ColumnValue end)
        {
            Begin = begin;
            End = end;
        }
    }
}
