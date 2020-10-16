using Aliyun.OTS.DataModel.Search.Query;

namespace Aliyun.OTS.DataModel.Search.Sort
{
    /**
 * 一个嵌套的过滤器
 */
    public class NestedFilter
    {
        public string Path { get; set; }
        public IQuery Query { get; set; }

        public NestedFilter(string path, IQuery query)
        {
            this.Path = path;
            this.Query = query;
        }
    }
}
