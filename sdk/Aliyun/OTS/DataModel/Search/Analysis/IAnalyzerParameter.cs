using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Analysis
{
    public interface IAnalyzerParameter
    {
        ByteString Serialize();
    }
}
