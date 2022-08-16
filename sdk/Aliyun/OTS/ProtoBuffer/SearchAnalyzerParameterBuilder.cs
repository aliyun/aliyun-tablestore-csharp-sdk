using Aliyun.OTS.DataModel.Search.Analysis;
using Google.ProtocolBuffers;
using PB = com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.ProtoBuffer
{
    public class SearchAnalyzerParameterBuilder
    {
        public static PB.SingleWordAnalyzerParameter EncodingSingleWordAnalyzerParameter(SingleWordAnalyzerParameter param)
        {
            PB.SingleWordAnalyzerParameter.Builder builder = PB.SingleWordAnalyzerParameter.CreateBuilder();

            if (param.CaseSensitive.HasValue)
            {
                builder.SetCaseSensitive(param.CaseSensitive.Value);
            }
            
            if (param.DelimitWord.HasValue)
            {
                builder.SetDelimitWord(param.DelimitWord.Value);
            }

            return builder.Build();
        }

        public static PB.FuzzyAnalyzerParameter EncodingFuzzyAnalyzerParameter(FuzzyAnalyzerParameter param)
        {
            PB.FuzzyAnalyzerParameter.Builder builder = PB.FuzzyAnalyzerParameter.CreateBuilder();

            if (param.MinChars.HasValue)
            {
                builder.SetMinChars(param.MinChars.Value);
            }
            
            if (param.MaxChars.HasValue)
            {
                builder.SetMaxChars(param.MaxChars.Value);
            }
            
            return builder.Build();
        }

        public static PB.SplitAnalyzerParameter EncodingSplitAnalyzerParameter(SplitAnalyzerParameter param)
        {
            PB.SplitAnalyzerParameter.Builder builder = PB.SplitAnalyzerParameter.CreateBuilder();

            builder.SetDelimiter(param.Delimiter);

            return builder.Build();
        }
    }
}
