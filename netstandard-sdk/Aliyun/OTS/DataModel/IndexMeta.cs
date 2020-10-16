using System.Collections.Generic;

namespace Aliyun.OTS.DataModel
{
    /// <summary>
    /// 索引表的结构信息，包含索引表的名称以及索引表的主键及预定义列定义
    /// </summary>
    public class IndexMeta
    {
        public string IndexName { get; set; }

        public List<string> PrimaryKey { get; set; }

        public List<string> DefinedColumns { get; set; }

        public IndexUpdateMode IndexUpdateModel { get; set; }

        public IndexType IndexType { get; set; }

        public IndexMeta(string indexName)
        {
            this.IndexName = indexName;
        }
    }
}
