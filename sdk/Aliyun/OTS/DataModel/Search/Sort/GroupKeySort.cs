namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class GroupKeySort
    {
        public SortOrder Order { get; set; }

        public GroupKeySort()
        {
        }

        public GroupKeySort(SortOrder order)
        {
            Order = order;
        }
    }
}
