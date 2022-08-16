namespace Aliyun.OTS.DataModel.Search.Sort
{
    public class FieldSort : ISorter
    {
        public static readonly ColumnValue FIRST_WHEN_MISSING = new ColumnValue("_first");

        public static readonly ColumnValue LAST_WHEN_MISSING = new ColumnValue("_last");

        private SortOrder order = SortOrder.ASC;

        public string FieldName { get; set; }

        public SortMode Mode { get; set; }

        public NestedFilter NestedFilter { get; set; }

        public ColumnValue MissingValue { get; set; }

        public string MissingField { get; set; }

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

