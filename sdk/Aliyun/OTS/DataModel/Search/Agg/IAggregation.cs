using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Agg
{
    public interface IAggregation
    {
        string GetAggName();

        AggregationType GetAggType();

        ByteString Serialize();
    }
}
