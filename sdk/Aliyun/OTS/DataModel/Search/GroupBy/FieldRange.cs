namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    /// <summary>
    /// 字段的范围
    /// </summary>
    public class FieldRange
    {
        public ColumnValue Min { get; set; }

        public ColumnValue Max { get; set; }

        public FieldRange()
        {
        }

        public FieldRange(ColumnValue min, ColumnValue max)
        {
            Min = min;
            Max = max;
        }
    }
}
