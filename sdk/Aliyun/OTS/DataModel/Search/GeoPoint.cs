namespace Aliyun.OTS.DataModel.Search
{
    /// <summary>
    /// 经纬度
    /// </summary>
    public class GeoPoint
    {
        public double Lat { get; set; }
        public double Lon { get; set; }

        public GeoPoint(double lat, double lon)
        {
            this.Lat = lat;
            this.Lon = lon;
        }
    }
}
