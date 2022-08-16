namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public class Range
    {
        public double? From { get; set; }

        public double? To { get; set; }

        public Range()
        {
        }

        public Range(double? from, double? to)
        {
            if (from.HasValue)
            {
                From = from.Value;
            }

            if (to.HasValue)
            {
                To = to;
            }
        }
    }
}
