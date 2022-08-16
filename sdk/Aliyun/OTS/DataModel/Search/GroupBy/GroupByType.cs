namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public enum GroupByType
    {
        /// <summary>
        /// 根据field进行group by
        /// </summary>
        GroupByField,
        /// <summary>
        /// 根据范围进行group by
        /// </summary>
        GroupByRange,
        /// <summary>
        /// 根据filter进行group by
        /// </summary>
        GroupByFilter,
        /// <summary>
        /// 根据经纬度进行group by
        /// </summary>
        GroupByGeoDistance,
        /// <summary>
        /// 对数据进行直方图统计
        /// </summary>
        GroupByHistogram
    }
}
