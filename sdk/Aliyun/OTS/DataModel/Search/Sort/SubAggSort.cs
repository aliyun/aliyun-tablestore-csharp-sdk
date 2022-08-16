namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class SubAggSort
    {
        public SortOrder Order { get; set; }

        public string SubAggName { get; set; }

        public SubAggSort()
        {
        }

        public SubAggSort(SortOrder order, string subAggName)
        {
            Order = order;
            SubAggName = subAggName;
        }
    }
}
