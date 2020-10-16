using System;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public interface IResultParser
    {
        Object getObject(GetRowRequest response);
    }
}
