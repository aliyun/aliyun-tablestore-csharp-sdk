namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class PrimaryKeySort : ISorter
    {
        public SortOrder Order { get; set; }

        public PrimaryKeySort() { }

        public PrimaryKeySort(SortOrder order)
        {
            this.Order = order;
        }
    }
}
