namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class GroupBySorter
    {
        public GroupKeySort GroupKeySort { get; set; }

        public RowCountSort RowCountSort { get; set; }

        public SubAggSort SubAggSort { get; set; }

        public GroupBySorter()
        {
        }

        public GroupBySorter(GroupKeySort groupKeySort, RowCountSort rowCountSort, SubAggSort subAggSort)
        {
            GroupKeySort = groupKeySort;
            RowCountSort = rowCountSort; ;
            SubAggSort = subAggSort;
        }
    }
}
