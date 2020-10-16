using System;
namespace Aliyun.OTS.DataModel
{
    public interface IRow : IComparable<IRow>
    {
        PrimaryKey GetPrimaryKey();
    }
}
