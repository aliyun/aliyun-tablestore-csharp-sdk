namespace Aliyun.OTS.Response
{
    public class ComputeSplitsResponse : OTSResponse
    {
        public byte[] SessionId { get; set; }

        public int? SplitsSize { get; set; }
    }
}
