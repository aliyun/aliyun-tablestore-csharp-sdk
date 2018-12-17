namespace Aliyun.OTS.DataModel.Search
{
    public enum FieldType
	{
		LONG,
		DOUBLE,
		BOOLEAN,
		/// <summary>
		/// 字符串类型，同Text的区别是keyword不分词，一般作为一个整体，如果想进行聚合统计分析，请使用该类型。
		/// </summary>
		KEYWORD,
		/// <summary>
		/// 字符串类型，同keyword的区别是text会进行分词，一般在模糊查询的场景使用。
		/// </summary>
		TEXT,
		NESTED,
		GEO_POINT
	}
}
