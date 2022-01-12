package no.unit.nva.search.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchResponse;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;
import static no.unit.nva.search.IndexedDocumentsJsonPointers.SOURCE_JSON_POINTER;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

@JsonPropertyOrder({"@context", "id", "took","email", "total", "hits" })
public class SearchResourcesResponse {

    public static final String TOTAL_JSON_POINTER = "/hits/total/value";
    public static final String TOOK_JSON_POINTER = "/took";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final URI DEFAULT_SEARCH_CONTEXT = URI.create("https://api.nva.unit.no/resources/search");
    public static final String QUERY_PARAMETER = "query";

    @JsonProperty("@context")
    private final URI context;
    private final URI id;
    private final int took;
    private final int total;
    private final List<JsonNode> hits;

    /**
     * Creates a SearchResourcesResponse with given properties.
     */
    @JacocoGenerated
    @JsonCreator
    public SearchResourcesResponse(@JsonProperty("@context") URI context,
                                   @JsonProperty("id") URI id,
                                   @JsonProperty("took") int took,
                                   @JsonProperty("total") int total,
                                   @JsonProperty("hits") List<JsonNode> hits) {
        this.context = context;
        this.id = id;
        this.took = took;
        this.total = total;
        this.hits = hits;
    }

    protected SearchResourcesResponse(Builder builder) {
        this.context = builder.context;
        this.id = builder.id;
        this.took = builder.took;
        this.total = builder.total;
        this.hits = builder.hits;
    }

    @JacocoGenerated
    public URI getContext() {
        return context;
    }

    @JacocoGenerated
    public URI getId() {
        return id;
    }

    @JacocoGenerated
    public int getTook() {
        return took;
    }

    @JacocoGenerated
    public int getTotal() {
        return total;
    }

    @JacocoGenerated
    public List<JsonNode> getHits() {
        return hits;
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SearchResourcesResponse that = (SearchResourcesResponse) o;
        return     took == that.took
                && total == that.total
                && Objects.equals(context, that.context)
                && Objects.equals(id, that.id)
                && Objects.equals(hits, that.hits);
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(context, id, took, total, hits);
    }

    public static final class Builder {

        private URI context;
        private URI id;
        private int took;
        private int total;
        private List<JsonNode> hits;

        public Builder withContext(URI context) {
            this.context = context;
            return this;
        }

        public Builder withId(URI id) {
            this.id = id;
            return this;
        }

        public Builder withTook(int took) {
            this.took = took;
            return this;
        }

        public Builder withTotal(int total) {
            this.total = total;
            return this;
        }

        public Builder withHits(List<JsonNode> hits) {
            this.hits = hits;
            return this;
        }

        public SearchResourcesResponse build() {
            return new SearchResourcesResponse(this);
        }

    }

    public static SearchResourcesResponse fromSearchResponse(SearchResponse searchResponse, URI id) {
        List<JsonNode> sourcesList = extractSourcesList(searchResponse);
        Long total = searchResponse.getHits().getTotalHits().value;
        Long took = searchResponse.getTook().duration();

        return new SearchResourcesResponse.Builder()
                .withContext(DEFAULT_SEARCH_CONTEXT)
                .withId(id)
                .withHits(sourcesList)
                .withTotal(total.intValue())
                .withTook(took.intValue())
                .build();
    }

    private static List<JsonNode> extractSourcesList(SearchResponse searchResponse) {
        return Arrays.stream(searchResponse.getHits().getHits())
                .map(hit -> hit.getSourceAsMap())
                .map(source -> objectMapperWithEmpty.convertValue(source, JsonNode.class))
                .collect(Collectors.toList());
    }

    public static SearchResourcesResponse toSearchResourcesResponse(
            URI requestUri, String searchTerm, String body) {

        JsonNode values = attempt(() -> objectMapperWithEmpty.readTree(body)).orElseThrow();

        List<JsonNode> sourceList = extractSourceList(values);
        int total = intFromNode(values, TOTAL_JSON_POINTER);
        int took = intFromNode(values, TOOK_JSON_POINTER);
        URI searchResultId = createIdWithQuery(requestUri, searchTerm);
        return new SearchResourcesResponse.Builder()
                .withContext(DEFAULT_SEARCH_CONTEXT)
                .withId(searchResultId)
                .withTook(took)
                .withTotal(total)
                .withHits(sourceList)
                .build();
    }

    public static URI createIdWithQuery(URI requestUri, String searchTerm) {
        UriWrapper wrapper = new UriWrapper(requestUri);
        if (nonNull(searchTerm)) {
            wrapper = wrapper.addQueryParameter(QUERY_PARAMETER, searchTerm);
        }
        return wrapper.getUri();
    }

    private static List<JsonNode> extractSourceList(JsonNode record) {
        return toStream(record.at(HITS_JSON_POINTER))
                .map(SearchResourcesResponse::extractSourceStripped)
                .collect(Collectors.toList());
    }

    private static JsonNode extractSourceStripped(JsonNode record) {
        return record.at(SOURCE_JSON_POINTER);
    }


    private static Stream<JsonNode> toStream(JsonNode node) {
        return StreamSupport.stream(node.spliterator(), false);
    }

    private static int intFromNode(JsonNode jsonNode, String jsonPointer) {
        JsonNode json = jsonNode.at(jsonPointer);
        return isPopulated(json) ? json.asInt() : 0;
    }

    private static boolean isPopulated(JsonNode json) {
        return !json.isNull() && !json.asText().isBlank();
    }

}
