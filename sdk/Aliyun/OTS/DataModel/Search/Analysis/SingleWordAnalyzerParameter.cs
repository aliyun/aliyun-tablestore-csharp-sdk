using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Analysis
{
    public class SingleWordAnalyzerParameter : IAnalyzerParameter
    {
        /// <summary>
        /// 是否大小写敏感
        /// </summary>
        public bool? CaseSensitive { get; set; }

        /// <summary>
        /// 是否分割数字
        /// </summary>
        public bool? DelimitWord { get; set; }

        public SingleWordAnalyzerParameter() { }

        public SingleWordAnalyzerParameter(bool? caseSensitive = null, bool? delimitWord = null)
        {
            if (caseSensitive.HasValue)
            {
                CaseSensitive = caseSensitive.Value;
            }

            if (delimitWord.HasValue)
            {
                DelimitWord = delimitWord.Value;
            }
        }

        public void SetCaseSensitive(bool caseSensitive)
        {
            CaseSensitive = caseSensitive;
        }

        public void SetDelimitWord(bool delimitWord)
        {
            DelimitWord = delimitWord;
        }

        public ByteString Serialize()
        {
            return SearchAnalyzerParameterBuilder.EncodingSingleWordAnalyzerParameter(this).ToByteString();
        }
    }
}
