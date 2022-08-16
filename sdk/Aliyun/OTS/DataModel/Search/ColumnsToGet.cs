using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search
{
    public class ColumnsToGet
    {
        public List<string> Columns { get; set; }
        public bool ReturnAll { get; set; }
        public bool ReturnAllFromIndex { get; set; }
    }
}
