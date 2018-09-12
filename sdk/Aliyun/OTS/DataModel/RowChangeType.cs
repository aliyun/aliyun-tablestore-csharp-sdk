namespace Aliyun.OTS.DataModel
{
    public enum RowChangeType
    {
        /// <summary>
        /// 代表写入该Column的某个特定版本的值。
        /// </summary>
        PUT,

        /// <summary>
        /// 代表删除该Column的某个特定版本，版本号的时间戳等于{@link Column#timestamp}。
        /// </summary>
        DELETE,

        /// <summary>
        /// 代表删除该Column的所有版本的值。
        /// </summary>
        DELETE_ALL
    }
}
