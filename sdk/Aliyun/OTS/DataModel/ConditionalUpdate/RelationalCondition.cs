using System;

namespace Aliyun.OTS.DataModel.ConditionalUpdate
{
    public class RelationalCondition : ColumnCondition
    {
        public enum CompareOperator
        {
            EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
        }

        public CompareOperator Operator { get; set; }
        public string ColumnName { get;set;}
        public ColumnValue ColumnValue { get; set; }
        public bool PassIfMissing {get;set;}

        public RelationalCondition(string columnName, CompareOperator oper, ColumnValue columnValue)
        {
            Operator = oper;
            ColumnName = columnName;
            ColumnValue = columnValue;
            PassIfMissing = true;
        }

        public new ColumnConditionType GetType()
        {
            return ColumnConditionType.RELATIONAL_CONDITION;
        }
    }
}
