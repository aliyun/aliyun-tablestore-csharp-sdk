using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Search.Query
{
    /**
   * IQuery接口，具体介绍请查看具体的实现类的说明
   */
    public interface IQuery
    {

        QueryType GetQueryType();

        ByteString Serialize();

    }
}
