﻿namespace Aliyun.OTS.DataModel.Search.Query
{
    /**
  * SearchIndex支持的Query类型
  */
    public enum QueryType
    {
        QueryType_None,
        QueryType_MatchQuery,
        QueryType_MatchPhraseQuery,
        QueryType_TermQuery,
        QueryType_RangeQuery,
        QueryType_PrefixQuery,
        QueryType_BoolQuery,
        QueryType_ConstScoreQuery,
        QueryType_FunctionScoreQuery,
        QueryType_NestedQuery,
        QueryType_WildcardQuery,
        QueryType_MatchAllQuery,
        QueryType_GeoBoundingBoxQuery,
        QueryType_GeoDistanceQuery,
        QueryType_GeoPolygonQuery,
        QueryType_TermsQuery,
        QueryType_ExistsQuery
    }
}
