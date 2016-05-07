using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.ConditionalUpdate
{
    public class CompositeCondition : ColumnCondition
    {
        public enum LogicOperator
        {
            NOT, AND, OR
        }

        private List<ColumnCondition> subConditions;

        public LogicOperator Type { get; set; }
        public List<ColumnCondition> SubConditions
        {
            get { return subConditions; }
        }

        public CompositeCondition(LogicOperator type)
        {
            Type = type;
            subConditions = new List<ColumnCondition>();
        }
        
        public CompositeCondition AddCondition(ColumnCondition condition)
        {
            subConditions.Add(condition);
            return this;
        }

        public void Clear() { subConditions.Clear(); }

        public new ColumnConditionType GetType()
        {
            return ColumnConditionType.COMPOSITE_CONDITION;
        }
    }
}
