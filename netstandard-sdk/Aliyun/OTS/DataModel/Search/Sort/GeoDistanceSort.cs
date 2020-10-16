using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class GeoDistanceSort : ISorter
    {
        /**
     * 排序的字段
     */
        public string FieldName { get; set; }
        /**
         * 排序的地理位置点
         */
        public List<string> Points { get; set; }
        /**
         * 升序或降序
         */
        public SortOrder Order { get; set; }
        /**
         * 多值字段的排序依据
         */
        public SortMode Mode { get; set; }
        /**
         * 计算两点距离的算法
         */
        public GeoDistanceType DistanceType { get; set; }
        /**
         * 嵌套的过滤器
         */
        public NestedFilter NestedFilter { get; set; }

        public GeoDistanceSort(String fieldName, List<String> points)
        {
            this.FieldName = fieldName;
            this.Points = points;
        }
    }
}
