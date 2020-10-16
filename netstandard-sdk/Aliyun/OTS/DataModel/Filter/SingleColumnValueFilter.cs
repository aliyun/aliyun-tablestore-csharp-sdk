using System.IO;
using Google.ProtocolBuffers;
using System;
using PB = com.alicloud.openservices.tablestore.core.protocol;
namespace Aliyun.OTS.DataModel.Filter
{
    /// <summary>
    ///   TableStore查询操作时使用的过滤器，SingleColumnValueFilter用于表示查询的行内数据列和数据值的比较关系。
    ///   <p>可以表示的列与值的比较关系包括：EQUAL(=), NOT_EQUAL(!=), GREATER_THAN(&gt;), GREATER_EQUAL(&gt;=), LESS_THAN(&lt;)以及LESS_EQUAL(&lt;=)。</p>
    ///   <p>由于TableStore一行的属性列不固定，有可能存在有filter条件的列在该行不存在的情况，这时{@link SingleColumnValueFilter#passIfMissing}参数控制在这种情况下对该行的过滤结果。</p>
    ///   如果设置
    ///   {@link SingleColumnValueFilter#passIfMissing}为true，则若列在该行中不存在，则返回该行；
    ///   如果设置{@ref SingleColumnValueFilter#passIfMissing}为false，则若列在该行中不存在，则不返回该行。
    ///   默认值为true。
    ///   < p > 由于TableStore的属性列可能有多个版本，有可能存在该列的一个版本的值与给定值匹配但是另一个版本的值不匹配的情况，</ p >
    ///   这时{@link SingleColumnValueFilter#latestVersionsOnly}参数控制在这种情况下对该行的过滤结果。
    ///   如果设置{@link SingleColumnValueFilter#latestVersionsOnly}为true，则只会对最新版本的值进行比较，否则会对该列的所有版本(最新的max_versions个)进行比较，
    ///   只要有一个版本的值匹配就认为条件成立。默认值为true。
    /// </summary>
    public class SingleColumnValueFilter : IFilter
    {
        public CompareOperator CompareOperator { get; set; }
        public string ColumnName { get; set; }
        public ColumnValue ColumnValue { get; set; }
        public bool PassIfMissing { get; set;}
        public bool LatestVersionsOnly { get; set; }

        /// <summary>
        /// 构造函数。
        /// </summary>
        /// <param name="columnName">列的名称</param>
        /// <param name="compareOperator">比较函数</param>
        /// <param name="columnValue">列的值</param>
        public SingleColumnValueFilter(string columnName,  CompareOperator compareOperator, ColumnValue columnValue)
        {
            ColumnName = columnName;
            CompareOperator = compareOperator;
            ColumnValue = columnValue;
            PassIfMissing = true;
            LatestVersionsOnly = true;
        }

        public FilterType GetFilterType()
        {
            return FilterType.SINGLE_COLUMN_VALUE_FILTER;
        }

        public ByteString Serialize()
        {
            return BuildSingleColumnValueFilter(this);
        }

        private static ByteString BuildSingleColumnValueFilter(SingleColumnValueFilter filter)
        {
            PB.SingleColumnValueFilter.Builder builder = PB.SingleColumnValueFilter.CreateBuilder();
            builder.SetColumnName(filter.ColumnName);
            builder.SetComparator(ToComparatorType(filter.CompareOperator));
            try
            {
                builder.SetColumnValue(ByteString.CopyFrom(PB.PlainBufferBuilder.BuildColumnValueWithoutLengthPrefix(filter.ColumnValue)));
            }
            catch (IOException e)
            {
                throw new OTSClientException("Bug: serialize column value failed." + e.Message);
            }

            builder.SetFilterIfMissing(!filter.PassIfMissing);
            builder.SetLatestVersionOnly(filter.LatestVersionsOnly);

            return builder.Build().ToByteString();
        }

        private static PB.ComparatorType ToComparatorType(CompareOperator compareOperator)
        {
            switch (compareOperator) {
                case CompareOperator.EQUAL:
                    return PB.ComparatorType.CT_EQUAL;
                case CompareOperator.NOT_EQUAL:
                    return PB.ComparatorType.CT_NOT_EQUAL;
                case CompareOperator.GREATER_THAN:
                    return PB.ComparatorType.CT_GREATER_THAN;
                case CompareOperator.GREATER_EQUAL:
                    return PB.ComparatorType.CT_GREATER_EQUAL;
                case CompareOperator.LESS_THAN:
                    return PB.ComparatorType.CT_LESS_THAN;
                case CompareOperator.LESS_EQUAL:
                    return PB.ComparatorType.CT_LESS_EQUAL;
                default:
                    throw new ArgumentException("Unknown compare operator: " + compareOperator);
            }
        }
    }
}
