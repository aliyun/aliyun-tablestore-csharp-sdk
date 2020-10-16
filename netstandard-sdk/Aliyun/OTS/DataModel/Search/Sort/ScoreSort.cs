namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class ScoreSort : ISorter
    {
        private SortOrder order = SortOrder.DESC;

        public SortOrder Order
        {
            get
            {
                return order;
            }
            set
            {
                order = value;
            }
        }
    }
}
