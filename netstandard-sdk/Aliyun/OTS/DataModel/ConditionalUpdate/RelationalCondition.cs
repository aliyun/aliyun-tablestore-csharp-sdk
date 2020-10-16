using Google.ProtocolBuffers;
using Aliyun.OTS.DataModel.Filter;

namespace Aliyun.OTS.DataModel.ConditionalUpdate
{
    public class RelationalCondition : IColumnCondition
    {
        public CompareOperator Operator { get; set; }
        public string ColumnName { get;set;}
        public ColumnValue ColumnValue { get; set; }
        public bool PassIfMissing {get;set;}
        public bool LatestVersionsOnly { get; set; }

        public RelationalCondition(string columnName, CompareOperator oper, ColumnValue columnValue)
        {
            Operator = oper;
            ColumnName = columnName;
            ColumnValue = columnValue;
            PassIfMissing = true;
            LatestVersionsOnly = true;
        }

        public ColumnConditionType GetConditionType()
        {
            return ColumnConditionType.RELATIONAL_CONDITION;
        }

        public ByteString Serialize()
        {

            return ToFilter().Serialize();
        }

        public IFilter ToFilter()
        {

            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(ColumnName, Operator, ColumnValue)
            {
                LatestVersionsOnly = LatestVersionsOnly,
                PassIfMissing = PassIfMissing
            };

            return singleColumnValueFilter;
        }
    }
}
