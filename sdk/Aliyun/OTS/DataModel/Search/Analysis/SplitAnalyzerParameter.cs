using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Analysis
{
    public class SplitAnalyzerParameter : IAnalyzerParameter
    {
        /// <summary>
        /// 分隔符，默认是空白字符，可以自定义分隔符
        /// </summary>
        public string Delimiter { get; set; }

        public SplitAnalyzerParameter() { }

        public SplitAnalyzerParameter(string delimiter)
        {
            Delimiter = delimiter;
        }

        public ByteString Serialize()
        {
            return SearchAnalyzerParameterBuilder.EncodingSplitAnalyzerParameter(this).ToByteString();
        }
    }
}
