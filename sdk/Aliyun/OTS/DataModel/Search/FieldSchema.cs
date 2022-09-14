using Aliyun.OTS.DataModel.Search.Analysis;
using System;
using System.Collections.Generic;

namespace Aliyun.OTS.DataModel.Search
{
    public enum Analyzer
    {
        SingleWord,
        MaxWord,
        MinWord,
        Fuzzy,
        Split
    }

    public class FieldSchema
    {
        /// <summary>
        /// 字段名
        /// </summary>
        public string FieldName { get; set; }
        /// <summary>
        ///  字段类型,详见{@link FieldType}
        /// </summary>
        public FieldType FieldType { get; set; }
        /// <summary>
        /// 是否开启索引，默认开启
        /// </summary>
        public Boolean index = true;
        /// <summary>
        /// 倒排索引的配置选项
        /// </summary>
        public IndexOptions? IndexOptions { get; set; }
        /// <summary>
        /// 分词器设置
        /// </summary>
        public Analyzer? Analyzer { get; set; }
        /// <summary>
        /// 分词参数
        /// </summary>
        public IAnalyzerParameter AnalyzerParameter { get; set; }
        /// <summary>
        /// 是否开启排序和聚合功能
        /// </summary>
        public bool EnableSortAndAgg { get; set; }
        /// <summary>
        ///  附加存储，是否在SearchIndex中附加存储该字段的值。
        ///开启后，可以直接从SearchIndex中读取该字段的值，而不必反查主表，可用于查询性能优化。
        /// </summary>
        public bool Store { get; set; }
        /// <summary>
        /// 存的值是否是一个数组
        /// </summary>
        public bool IsArray { get; set; }
        /// <summary>
        /// 如果 FiledType 是 NESTED ，则可使用该字段，声明一个嵌套的FieldSchema
        /// </summary>
        public List<FieldSchema> SubFieldSchemas { get; set; }
        /// <summary>
        ///  是否是虚拟字段
        /// </summary>
        public bool? IsVirtualField { get; set; }
        /// <summary>
        /// 虚拟字段对应的原始字段。
        /// 当前仅支持设置一个原始字段
        /// </summary>
        public List<string> SourceFieldNames { get; set; }
        /// <summary>
        /// 当字段类型是<see cref="FieldType.DATA"/>日期类型时，可以定义该日期支持的格式
        /// </summary>
        public List<string> DateFormats { get; set; }


        public FieldSchema(string fieldName, FieldType fieldType)
        {
            FieldName = fieldName;
            FieldType = fieldType;
        }
    }
}
