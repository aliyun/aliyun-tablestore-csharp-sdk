namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class FieldSort : ISorter
    {
        private SortOrder order = SortOrder.ASC;

        public string FieldName { get; set; }
        public SortMode Mode { get; set; }
        public NestedFilter NestedFilter { get; set; }

        public SortOrder Order
        {
            get { return order; }
            set { order = value; }
        }

        public FieldSort(string fieldName, SortOrder order)
        {
            this.FieldName = fieldName;
            this.Order = order;
        }
    }
}

