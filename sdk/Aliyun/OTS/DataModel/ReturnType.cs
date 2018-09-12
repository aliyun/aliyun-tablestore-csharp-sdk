using System;
namespace Aliyun.OTS.DataModel
{
    /**
  * 表示操作（PUT，UPDATE，DELETE）的返回结果中是否附带PK值，对于PK递增列，应该设置返回PK
  */
    public enum ReturnType
    {
        /// <summary>
        /// 不返回任何行数据。
        /// </summary>
        RT_NONE,

        /// <summary>
        /// 返回PK列的数据。
        /// </summary>
        RT_PK,
    }
}
