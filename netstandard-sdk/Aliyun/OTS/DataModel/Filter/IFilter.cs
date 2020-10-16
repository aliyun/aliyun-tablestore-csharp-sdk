using System;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Filter
{
    public interface IFilter
    {
        FilterType GetFilterType();

        ByteString Serialize();
    }
}
