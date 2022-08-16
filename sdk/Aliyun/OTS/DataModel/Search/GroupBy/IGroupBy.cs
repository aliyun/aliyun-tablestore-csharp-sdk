using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.GroupBy
{
    public interface IGroupBy
    {
        string GetGroupByName();

        GroupByType GetGroupByType();

        ByteString Serialize();
    }
}
