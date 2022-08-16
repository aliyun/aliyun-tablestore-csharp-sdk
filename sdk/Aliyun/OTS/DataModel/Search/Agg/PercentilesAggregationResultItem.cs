using System.Text;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public class PercentilesAggregationResultItem
    {
        public double Key { get; set; }

        public ColumnValue Value { get; set; }

        public string Serialize()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(Key);
            sb.Append(" : ");
            sb.Append(Value.ToString());
            return sb.ToString();
        }
    }
}
