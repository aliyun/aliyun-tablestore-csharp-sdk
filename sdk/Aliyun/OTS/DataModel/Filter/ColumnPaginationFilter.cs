using System;
using Google.ProtocolBuffers;
using PB = com.alicloud.openservices.tablestore.core.protocol;

namespace Aliyun.OTS.DataModel.Filter
{
    public class ColumnPaginationFilter : IFilter
    {
        public int Limit { get; set; }
        public int Offset { get; set; }

        public ColumnPaginationFilter(int limit) : this(limit, 0)
        {
        }

        public ColumnPaginationFilter(int limit, int offset)
        {
            this.Limit = limit;
            this.Offset = offset;
        }

        public FilterType GetFilterType()
        {
            return FilterType.COLUMN_PAGINATION_FILTER;
        }

        public ByteString Serialize()
        {
            PB.ColumnPaginationFilter.Builder builder = PB.ColumnPaginationFilter.CreateBuilder();
            builder.SetLimit(this.Limit);
            builder.SetOffset(this.Offset);
            return builder.Build().ToByteString();
        }
    }
}
