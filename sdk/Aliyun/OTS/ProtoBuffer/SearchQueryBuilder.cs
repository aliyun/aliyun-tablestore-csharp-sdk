using System;
using Aliyun.OTS.DataModel.Search.Query;
using Aliyun.OTS.ProtoBuffer;
using Google.ProtocolBuffers;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class SearchQueryBuilder
    {
        public static QueryType BuildQueryType(Aliyun.OTS.DataModel.Search.Query.QueryType type)
        {
            switch (type)
            {
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_MatchQuery:
                    return QueryType.MATCH_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_MatchPhraseQuery:
                    return QueryType.MATCH_PHRASE_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_TermQuery:
                    return QueryType.TERM_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_TermsQuery:
                    return QueryType.TERMS_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_RangeQuery:
                    return QueryType.RANGE_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_PrefixQuery:
                    return QueryType.PREFIX_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_BoolQuery:
                    return QueryType.BOOL_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_ConstScoreQuery:
                    return QueryType.CONST_SCORE_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_FunctionScoreQuery:
                    return QueryType.FUNCTION_SCORE_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_NestedQuery:
                    return QueryType.NESTED_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_WildcardQuery:
                    return QueryType.WILDCARD_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_MatchAllQuery:
                    return QueryType.MATCH_ALL_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_GeoBoundingBoxQuery:
                    return QueryType.GEO_BOUNDING_BOX_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_GeoDistanceQuery:
                    return QueryType.GEO_DISTANCE_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_GeoPolygonQuery:
                    return QueryType.GEO_POLYGON_QUERY;
                case Aliyun.OTS.DataModel.Search.Query.QueryType.QueryType_ExistsQuery:
                    return QueryType.EXISTS_QUERY;
                default:
                    throw new ArgumentException("unknown queryType: " + type.ToString());
            }
        }

        public static Query BuildQuery(IQuery query)
        {
            Query.Builder builder = Query.CreateBuilder();
            builder.SetType(BuildQueryType(query.GetQueryType()));
            builder.SetQuery_(query.Serialize());
            return builder.Build();
        }

        public static MatchAllQuery BuildMatchAllQuery()
        {
            MatchAllQuery.Builder builder = MatchAllQuery.CreateBuilder();
            return builder.Build();
        }

        public static MatchQuery BuildMatchQuery(Aliyun.OTS.DataModel.Search.Query.MatchQuery query)
        {
            MatchQuery.Builder builder = MatchQuery.CreateBuilder();

            builder.SetFieldName(query.FieldName);
            builder.SetText(query.Text);
            builder.SetWeight(query.Weight);
            if (query.MinimumShouldMatch.HasValue)
            {
                builder.SetMinimumShouldMatch(query.MinimumShouldMatch.Value);
            }
            switch (query.Operator)
            {
                case Aliyun.OTS.DataModel.Search.Query.QueryOperator.AND:
                    builder.SetOperator(QueryOperator.AND);
                    break;
                case Aliyun.OTS.DataModel.Search.Query.QueryOperator.OR:
                    builder.SetOperator(QueryOperator.OR);
                    break;
                default:
                    throw new ArgumentException("unsupported operator:" + query.Operator);

            }

            return builder.Build();
        }

        public static MatchPhraseQuery BuildMatchPhraseQuery(Aliyun.OTS.DataModel.Search.Query.MatchPhraseQuery query)
        {
            MatchPhraseQuery.Builder builder = MatchPhraseQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetText(query.Text);
            builder.SetWeight(query.Weight);
            return builder.Build();
        }

        public static TermQuery BuildTermQuery(Aliyun.OTS.DataModel.Search.Query.TermQuery query)
        {
            TermQuery.Builder builder = TermQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetTerm(ByteString.CopyFrom(SearchVariantType.ToVariant(query.Term)));
            builder.SetWeight(query.Weight);
            return builder.Build();
        }

        public static TermsQuery BuildTermsQuery(Aliyun.OTS.DataModel.Search.Query.TermsQuery query)
        {
            TermsQuery.Builder builder = TermsQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetWeight(query.Weight);
            if (query.Terms == null)
            {
                throw new ArgumentException("terms is null");
            }
            foreach (var item in query.Terms)
            {
                builder.AddTerms(ByteString.CopyFrom(SearchVariantType.ToVariant(item)));
            }
            return builder.Build();
        }

        public static RangeQuery BuildRangeQuery(Aliyun.OTS.DataModel.Search.Query.RangeQuery query)
        {
            RangeQuery.Builder builder = RangeQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            if (query.From != null)
            {
                builder.SetRangeFrom(ByteString.CopyFrom(SearchVariantType.ToVariant(query.From)));
                builder.SetIncludeLower(query.IncludeLower);
            }
            if (query.To != null)
            {
                builder.SetRangeTo(ByteString.CopyFrom(SearchVariantType.ToVariant(query.To)));
                builder.SetIncludeUpper(query.IncludeUpper);
            }
            return builder.Build();
        }

        public static PrefixQuery BuildPrefixQuery(Aliyun.OTS.DataModel.Search.Query.PrefixQuery query)
        {
            PrefixQuery.Builder builder = PrefixQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetPrefix(query.Prefix);
            builder.SetWeight(query.Weight);
            return builder.Build();
        }

        public static WildcardQuery BuildWildcardQuery(Aliyun.OTS.DataModel.Search.Query.WildcardQuery query)
        {
            WildcardQuery.Builder builder = WildcardQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetValue(query.Value);
            builder.SetWeight(query.Weight);
            return builder.Build();
        }

        public static BoolQuery BuildBoolQuery(Aliyun.OTS.DataModel.Search.Query.BoolQuery query)
        {
            BoolQuery.Builder builder = BoolQuery.CreateBuilder();
            if (query.MinimumShouldMatch.HasValue)
            {
                builder.SetMinimumShouldMatch(query.MinimumShouldMatch.Value);
            }
            if (query.MustQueries != null)
            {
                foreach (var q in query.MustQueries)
                {
                    builder.AddMustQueries(BuildQuery(q));
                }
            }
            if (query.MustNotQueries != null)
            {
                foreach (var q in query.MustNotQueries)
                {
                    builder.AddMustNotQueries(BuildQuery(q));
                }
            }
            if (query.ShouldQueries != null)
            {
                foreach (var q in query.ShouldQueries)
                {
                    builder.AddShouldQueries(BuildQuery(q));
                }
            }
            if (query.FilterQueries != null)
            {
                foreach (var q in query.FilterQueries)
                {
                    builder.AddFilterQueries(BuildQuery(q));
                }
            }
            return builder.Build();
        }

        public static ConstScoreQuery BuildConstScoreQuery(Aliyun.OTS.DataModel.Search.Query.ConstScoreQuery query)
        {

            ConstScoreQuery.Builder builder = ConstScoreQuery.CreateBuilder();
            builder.SetFilter(SearchQueryBuilder.BuildQuery(query.Filter));
            return builder.Build();
        }

        public static FieldValueFactor BuildFieldValueFactor(Aliyun.OTS.DataModel.Search.Query.FieldValueFactor fieldValueFactor)
        {
            FieldValueFactor.Builder builder = FieldValueFactor.CreateBuilder();
            builder.SetFieldName(fieldValueFactor.FieldName);
            return builder.Build();
        }

        public static FunctionScoreQuery BuildFunctionScoreQuery(Aliyun.OTS.DataModel.Search.Query.FunctionScoreQuery query)
        {
            FunctionScoreQuery.Builder builder = FunctionScoreQuery.CreateBuilder();
            builder.SetQuery(SearchQueryBuilder.BuildQuery(query.Query));
            builder.SetFieldValueFactor(BuildFieldValueFactor(query.FieldValueFactor));
            return builder.Build();
        }

        public static ScoreMode BuildScoreMode(Aliyun.OTS.DataModel.Search.Query.ScoreMode scoreMode)
        {
            switch (scoreMode)
            {
                case Aliyun.OTS.DataModel.Search.Query.ScoreMode.Max:
                    return ScoreMode.SCORE_MODE_MAX;
                case Aliyun.OTS.DataModel.Search.Query.ScoreMode.Min:
                    return ScoreMode.SCORE_MODE_MIN;
                case Aliyun.OTS.DataModel.Search.Query.ScoreMode.Avg:
                    return ScoreMode.SCORE_MODE_AVG;
                case Aliyun.OTS.DataModel.Search.Query.ScoreMode.Total:
                    return ScoreMode.SCORE_MODE_TOTAL;
                case Aliyun.OTS.DataModel.Search.Query.ScoreMode.None:
                    return ScoreMode.SCORE_MODE_NONE;
                default:
                    throw new ArgumentException("unknown scoreMode: " + scoreMode.ToString());
            }
        }

        public static NestedQuery BuildNestedQuery(Aliyun.OTS.DataModel.Search.Query.NestedQuery query)
        {
            NestedQuery.Builder builder = NestedQuery.CreateBuilder();
            builder.SetQuery(SearchQueryBuilder.BuildQuery(query.Query));
            builder.SetPath(query.Path);
            builder.SetScoreMode(BuildScoreMode(query.ScoreMode));
            builder.SetWeight(query.Weight);
            return builder.Build();
        }

        public static GeoBoundingBoxQuery BuildGeoBoundingBoxQuery(Aliyun.OTS.DataModel.Search.Query.GeoBoundingBoxQuery query)
        {
            GeoBoundingBoxQuery.Builder builder = GeoBoundingBoxQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetTopLeft(query.TopLeft);
            builder.SetBottomRight(query.BottomRight);
            return builder.Build();
        }

        public static GeoDistanceQuery BuildGeoDistanceQuery(Aliyun.OTS.DataModel.Search.Query.GeoDistanceQuery query)
        {
            GeoDistanceQuery.Builder builder = GeoDistanceQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.SetCenterPoint(query.CenterPoint);
            builder.SetDistance(query.DistanceInMeter);
            return builder.Build();
        }

        public static GeoPolygonQuery BuildGeoPolygonQuery(Aliyun.OTS.DataModel.Search.Query.GeoPolygonQuery query)
        {
            GeoPolygonQuery.Builder builder = GeoPolygonQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            builder.AddRangePoints(query.Points);
            return builder.Build();
        }

        public static ExistsQuery BuildExistQuery(Aliyun.OTS.DataModel.Search.Query.ExistsQuery query)
        {
            ExistsQuery.Builder builder = ExistsQuery.CreateBuilder();
            builder.SetFieldName(query.FieldName);
            return builder.Build();
        }
    }
}
