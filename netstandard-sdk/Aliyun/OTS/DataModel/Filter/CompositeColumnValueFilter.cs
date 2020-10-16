using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using PB = com.alicloud.openservices.tablestore.core.protocol;
using Google.ProtocolBuffers;

namespace Aliyun.OTS.DataModel.Filter
{
    /**
     * TableStore查询操作时使用的过滤器，CompositeColumnValueFilter用于表示ColumnValueFilter之间的逻辑关系条件，主要有NOT、AND和OR三种逻辑关系条件，其中NOT和AND表示二元或多元的关系，NOT只表示一元的关系。
     * <p>逻辑关系通过构造函数{@link CompositeColumnValueFilter#CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator)}的参数提供。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#NOT}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter有且只有一个。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#AND}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter必须大于等于两个。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#OR}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter必须大于等于两个。</p>
     */
    public class CompositeColumnValueFilter : IFilter
    {
        private readonly LogicOperator type;
        private readonly List<IFilter> filters;

        public CompositeColumnValueFilter(LogicOperator loType)
        {
            this.type = loType;
            this.filters = new List<IFilter>();
        }

        /// <summary>
        /// 增加逻辑关系组中的ColumnValueFilter。
        ///  <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#NOT}，有且只能添加一个ColumnValueFilter。</p>
        ///  <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#AND}，必须添加至少两个ColumnValueFilter。</p>
        ///  <p>若逻辑关系为{ @link CompositeColumnValueFilter.LogicOperator#OR}，必须添加至少两个ColumnValueFilter。</p>
        /// </summary>
        /// <returns>The Composite filter.</returns>
        /// <param name="filter">Filter.</param>
        public CompositeColumnValueFilter AddFilter(IFilter filter)
        {
            Contract.Requires(filter != null);
            this.filters.Add(filter);
            return this;
        }

        public void Clear()
        {
            this.filters.Clear();
        }

        /// <summary>
        /// 查看当前设置的逻辑关系
        /// </summary>
        /// <returns>逻辑关系符号</returns>
        public LogicOperator GetOperationType()
        {
            return this.type;
        }

        /// <summary>
        /// 返回逻辑关系组中的所有ColumnValueFilter。
        /// </summary>
        /// <returns>The sub filters.</returns>
        public List<IFilter> GetSubFilters()
        {
            return this.filters;
        }


        public FilterType GetFilterType()
        {
            return FilterType.COMPOSITE_COLUMN_VALUE_FILTER;
        }


        public ByteString Serialize()
        {
            return BuildCompositeColumnValueFilter(this);
        }

        private static ByteString BuildCompositeColumnValueFilter(CompositeColumnValueFilter filter)
        {
            PB.CompositeColumnValueFilter.Builder builder = PB.CompositeColumnValueFilter.CreateBuilder();
            builder.SetCombinator(ToPBLogicalOperator(filter.GetOperationType()));

            foreach (IFilter f in filter.GetSubFilters())
            {
                builder.AddSubFilters(ToPBFilter(f));
            }

            return builder.Build().ToByteString();
        }

        private static PB.LogicalOperator ToPBLogicalOperator(LogicOperator type)
        {
            switch (type)
            {
                case LogicOperator.NOT:
                    return PB.LogicalOperator.LO_NOT;
                case LogicOperator.AND:
                    return PB.LogicalOperator.LO_AND;
                case LogicOperator.OR:
                    return PB.LogicalOperator.LO_OR;
                default:
                    throw new ArgumentException("Unknown logic operation type: " + type);
            }
        }

        private static PB.Filter ToPBFilter(IFilter f)
        {
            PB.Filter.Builder builder = PB.Filter.CreateBuilder();
            builder.SetType(ToPBFilterType(f.GetFilterType()));
            builder.SetFilter_(f.Serialize());
            return builder.Build();
        }

        private static PB.FilterType ToPBFilterType(FilterType type)
        {
            switch (type)
            {
                case FilterType.COMPOSITE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_COMPOSITE_COLUMN_VALUE;
                case FilterType.SINGLE_COLUMN_VALUE_FILTER:
                    return PB.FilterType.FT_SINGLE_COLUMN_VALUE;
                case FilterType.COLUMN_PAGINATION_FILTER:
                    return PB.FilterType.FT_COLUMN_PAGINATION;
                default:
                    throw new ArgumentException("Unknown filter type: " + type);
            }
        }
    }
}
