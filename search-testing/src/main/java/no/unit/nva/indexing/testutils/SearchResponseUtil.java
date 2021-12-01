package no.unit.nva.indexing.testutils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

public final class SearchResponseUtil {

    private SearchResponseUtil() {

    }

    private static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        List<NamedXContentRegistry.Entry> entries = map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(
                        Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        return entries;
    }

    public static SearchResponse getSearchResponseFromJson(String jsonResponse) throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(getDefaultNamedXContents());
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                registry, IGNORE_DEPRECATIONS, jsonResponse)) {
            return SearchResponse.fromXContent(parser);
        }
    }
}
