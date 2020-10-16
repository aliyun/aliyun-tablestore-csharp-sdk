namespace Aliyun.OTS.DataModel
{
    public interface IMeasurable
    {
        /// <summary>
        /// 序列化后占用的数据大小
        /// </summary>
        /// <returns>序列化后占用的数据大小</returns>
        int GetDataSize();
    }
}
