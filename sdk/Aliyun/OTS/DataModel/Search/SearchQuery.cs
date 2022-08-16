using Aliyun.OTS.DataModel.Search.Agg;
using Aliyun.OTS.DataModel.Search.GroupBy;
using Aliyun.OTS.DataModel.Search.Query;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search
{
    public class SearchQuery
    {
        /// <summary>
        /// 分页起始数量
        /// </summary>
        public int? Offset { get; set; }
        /// <summary>
        /// 分页大小，即返回的行数
        /// </summary>
        public int? Limit { get; set; }
        /// <summary>
        /// 查询语句
        /// </summary>
        public IQuery Query { get; set; }
        /// <summary>
        /// 字段折叠
        /// 能够实现某个字段的结果去重。
        /// </summary>
        public Collapse Collapse { get; set; }
        /// <summary>
        /// 排序
        /// 设置结果的排序方式，该参数支持多字段排序</p>
        /// </summary>
        public Sort.Sort Sort { get; set; }
        /// <summary>
        /// 获取总行数，默认设置为false
        /// </summary>
        public bool GetTotalCount { get; set; }

        private byte[] token { get; set; }

        public byte[] Token
        {
            get
            {
                return token;
            }
            set
            {
                //Token中编码了Sort条件，所以设置Token时不需要设置Sort
                token = value;
                Sort = null;
            }
        }

        public List<IAggregation> AggregationList { get; set; }

        public List<IGroupBy> GroupByList { get; set; }

        public SearchQuery()
        {
            GetTotalCount = false;
        }
    }
}
