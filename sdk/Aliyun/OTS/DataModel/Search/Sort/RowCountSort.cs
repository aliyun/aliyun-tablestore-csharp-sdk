namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class RowCountSort
    {
        public SortOrder Order { get; set; }

        public RowCountSort()
        {
        }

        public RowCountSort(SortOrder order)
        {
            Order = order;
        }
    }
}
