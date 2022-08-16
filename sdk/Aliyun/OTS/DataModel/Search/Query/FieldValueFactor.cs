namespace Aliyun.OTS.DataModel.Search.Query
{
    /// <summary>
    /// field_value_factor的目的是通过文档中某个字段的值计算出一个分数,以此分数来影响文档的排序。请结合{@link FunctionScoreQuery} 使用。
    ///举例：HR管理系统的场景，我们想查名字中包含“王”、出生地包含“京”的人，但是想让结果根据根据身高排序。就可以把身高设置在FieldValueFactor中
    /// </summary>
    public class FieldValueFactor
    {
        public string FieldName { get; set; }

        public FieldValueFactor(string fieldName)
        {
            FieldName = fieldName;
        }
    }
}