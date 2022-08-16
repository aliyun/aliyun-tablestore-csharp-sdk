using System;
using System.Collections.Generic;
using com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchSortBuilder
    {

        public static SortOrder BuildSortOrder(DataModel.Search.Sort.SortOrder sortOrder)
        {
            switch (sortOrder)
            {
                case DataModel.Search.Sort.SortOrder.ASC:
                    return SortOrder.SORT_ORDER_ASC;
                case DataModel.Search.Sort.SortOrder.DESC:
                    return SortOrder.SORT_ORDER_DESC;
                default:
                    throw new ArgumentException("unknown sortOrder: " + sortOrder.ToString());
            }
        }

        public static SortMode BuildSortMode(DataModel.Search.Sort.SortMode sortMode)
        {
            switch (sortMode)
            {
                case DataModel.Search.Sort.SortMode.MIN:
                    return SortMode.SORT_MODE_MIN;
                case DataModel.Search.Sort.SortMode.MAX:
                    return SortMode.SORT_MODE_MAX;
                case DataModel.Search.Sort.SortMode.AVG:
                    return SortMode.SORT_MODE_AVG;
                default:
                    throw new ArgumentException("unknown sortOrder: " + sortMode.ToString());
            }
        }

        public static NestedFilter BuildNestedFilter(DataModel.Search.Sort.NestedFilter nestedFilter)
        {
            NestedFilter.Builder builder = NestedFilter.CreateBuilder();
            builder.SetPath(nestedFilter.Path);
            builder.SetFilter(SearchQueryBuilder.BuildQuery(nestedFilter.Query));
            return builder.Build();
        }

        public static FieldSort BuildFieldSort(DataModel.Search.Sort.FieldSort fieldSort)
        {
            FieldSort.Builder builder = FieldSort.CreateBuilder();

            builder.SetFieldName(fieldSort.FieldName);

            builder.SetOrder(BuildSortOrder(fieldSort.Order));

            builder.SetMode(BuildSortMode(fieldSort.Mode));

            if (fieldSort.NestedFilter != null)
            {
                builder.SetNestedFilter(BuildNestedFilter(fieldSort.NestedFilter));
            }

            if (fieldSort.MissingValue != null)
            {
                builder.MissingValue = ByteString.CopyFrom(SearchVariantType.ToVariant(fieldSort.MissingValue));
            }

            if (fieldSort.MissingField != null)
            {
                builder.MissingField = fieldSort.MissingField;
            }

            return builder.Build();
        }

        public static ScoreSort BuildScoreSort(DataModel.Search.Sort.ScoreSort scoreSort)
        {
            ScoreSort.Builder builder = ScoreSort.CreateBuilder();
            builder.SetOrder(BuildSortOrder(scoreSort.Order));
            return builder.Build();
        }

        public static GeoDistanceType BuildGeoDistanceType(DataModel.Search.Sort.GeoDistanceType geoDistanceType)
        {
            switch (geoDistanceType)
            {
                case DataModel.Search.Sort.GeoDistanceType.ARC:
                    return GeoDistanceType.GEO_DISTANCE_ARC;
                case DataModel.Search.Sort.GeoDistanceType.PLANE:
                    return GeoDistanceType.GEO_DISTANCE_PLANE;
                default:
                    throw new ArgumentException("unknown geoDistanceType: " + geoDistanceType.ToString());
            }
        }

        public static GeoDistanceSort BuildGeoDistanceSort(DataModel.Search.Sort.GeoDistanceSort geoDistanceSort)
        {
            GeoDistanceSort.Builder builder = GeoDistanceSort.CreateBuilder();
            builder.SetFieldName(geoDistanceSort.FieldName);
            if (geoDistanceSort.Points != null)
            {
                builder.AddRangePoints(geoDistanceSort.Points);
            }

            builder.SetOrder(BuildSortOrder(geoDistanceSort.Order));


            builder.SetMode(BuildSortMode(geoDistanceSort.Mode));


            builder.SetDistanceType(BuildGeoDistanceType(geoDistanceSort.DistanceType));

            if (geoDistanceSort.NestedFilter != null)
            {
                builder.SetNestedFilter(BuildNestedFilter(geoDistanceSort.NestedFilter));
            }
            return builder.Build();
        }

        public static PrimaryKeySort BuilderPrimarykeySort(DataModel.Search.Sort.PrimaryKeySort primaryKeySort)
        {
            var builder = PrimaryKeySort.CreateBuilder();
            builder.SetOrder(BuildSortOrder(primaryKeySort.Order));
            return builder.Build();
        }

        public static Sorter BuildSorter(DataModel.Search.Sort.ISorter sorter)
        {
            Sorter.Builder builder = Sorter.CreateBuilder();
            if (sorter is DataModel.Search.Sort.FieldSort)
            {
                builder.SetFieldSort(BuildFieldSort((DataModel.Search.Sort.FieldSort)sorter));
            }
            else if (sorter is DataModel.Search.Sort.ScoreSort)
            {
                builder.SetScoreSort(BuildScoreSort((DataModel.Search.Sort.ScoreSort)sorter));
            }
            else if (sorter is DataModel.Search.Sort.GeoDistanceSort)
            {
                builder.SetGeoDistanceSort(BuildGeoDistanceSort((DataModel.Search.Sort.GeoDistanceSort)sorter));
            }
            else if (sorter is DataModel.Search.Sort.PrimaryKeySort)
            {
                builder.SetPkSort(BuilderPrimarykeySort((DataModel.Search.Sort.PrimaryKeySort)sorter));
            }
            else
            {
                throw new ArgumentException("unknown sorter type: " + sorter.ToString());
            }
            return builder.Build();
        }

        public static Sort BuildSort(DataModel.Search.Sort.Sort sort)
        {
            Sort.Builder builder = Sort.CreateBuilder();
            foreach (var sorter in sort.Sorters)
            {
                builder.AddSorter(BuildSorter(sorter));
            }
            return builder.Build();
        }

        public static GroupBySort BuildGroupBySort(List<DataModel.Search.Sort.GroupBySorter> groupBySorters)
        {
            GroupBySort.Builder builder = GroupBySort.CreateBuilder();
            foreach (DataModel.Search.Sort.GroupBySorter groupBySorter in groupBySorters)
            {
                builder.AddSorters(BuildGroupBySorter(groupBySorter));
            }
            return builder.Build();
        }

        private static GroupBySorter BuildGroupBySorter(DataModel.Search.Sort.GroupBySorter groupBySorter)
        {
            GroupBySorter.Builder builder = GroupBySorter.CreateBuilder();

            if (groupBySorter.GroupKeySort != null)
            {
                builder.SetGroupKeySort(BuildGroupKeySort(groupBySorter.GroupKeySort));
            }

            if (groupBySorter.RowCountSort != null)
            {
                builder.SetRowCountSort(BuildRowCountSort(groupBySorter.RowCountSort));
            }

            if (groupBySorter.SubAggSort != null)
            {
                builder.SetSubAggSort(BuildSubAggSort(groupBySorter.SubAggSort));
            }

            return builder.Build();
        }

        private static GroupKeySort BuildGroupKeySort(DataModel.Search.Sort.GroupKeySort groupKeySort)
        {
            GroupKeySort.Builder builder = GroupKeySort.CreateBuilder();
            builder.SetOrder(BuildSortOrder(groupKeySort.Order));
            return builder.Build();
        }

        private static RowCountSort BuildRowCountSort(DataModel.Search.Sort.RowCountSort rowCountSort)
        {
            RowCountSort.Builder builder = RowCountSort.CreateBuilder();
            builder.SetOrder(BuildSortOrder(rowCountSort.Order));
            return builder.Build();
        }

        private static SubAggSort BuildSubAggSort(DataModel.Search.Sort.SubAggSort subAggSort)
        {
            SubAggSort.Builder builder = SubAggSort.CreateBuilder();
            builder.SetOrder(BuildSortOrder(subAggSort.Order));
            builder.SetSubAggName(subAggSort.SubAggName);
            return builder.Build();
        }
    }
}
