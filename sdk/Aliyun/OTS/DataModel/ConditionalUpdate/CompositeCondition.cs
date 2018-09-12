using System.Collections.Generic;
using Google.ProtocolBuffers;
using Aliyun.OTS.DataModel.Filter;

namespace Aliyun.OTS.DataModel.ConditionalUpdate
{
    public class CompositeCondition : IColumnCondition
    {
        private List<IColumnCondition> subConditions;

        public LogicOperator LogicOperator { get; set; }

        public List<IColumnCondition> SubConditions
        {
            get { return subConditions; }
        }

        public CompositeCondition(LogicOperator logicOperator)
        {
            LogicOperator = logicOperator;
            subConditions = new List<IColumnCondition>();
        }
        
        public CompositeCondition AddCondition(IColumnCondition condition)
        {
            subConditions.Add(condition);
            return this;
        }

        public void Clear() { subConditions.Clear(); }

        public ColumnConditionType GetConditionType()
        {
            return ColumnConditionType.COMPOSITE_CONDITION;
        }

        public ByteString Serialize()
        {
            return ToFilter().Serialize();
        }

        public IFilter ToFilter()
        {
            CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(LogicOperator);

            foreach (IColumnCondition condition in SubConditions)
            {
                compositeColumnValueFilter.AddFilter(condition.ToFilter());
            }

            return compositeColumnValueFilter;
        }
    }
}
