using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Analysis
{
    public class FuzzyAnalyzerParameter : IAnalyzerParameter
    {
        /// <summary>
        /// 最小字符切分单元
        /// </summary>
        public int? MinChars { get; set; }

        /// <summary>
        /// 最大字符切分单元
        /// </summary>
        public int? MaxChars { get; set; }

        public FuzzyAnalyzerParameter() { }

        public FuzzyAnalyzerParameter(int? minChars, int? maxChars)
        {
            if (minChars.HasValue)
            {
                MinChars = minChars.Value;
            }

            if (maxChars.HasValue)
            {
                MaxChars = maxChars.Value;
            }
        }

        public ByteString Serialize()
        {
            return SearchAnalyzerParameterBuilder.EncodingFuzzyAnalyzerParameter(this).ToByteString();
        }
    }
}
