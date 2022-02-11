package no.unit.nva.search.models;

import static java.util.Objects.nonNull;
import static no.unit.nva.search.IndexedDocumentsJsonPointers.SOURCE_JSON_POINTER;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchResponse;

public class SearchResourcesResponse {

    public static final String TOTAL_JSON_POINTER = "/hits/total/value";
    public static final String TOOK_JSON_POINTER = "/took";
    public static final String HITS_JSON_POINTER = "/hits/hits";
    public static final URI DEFAULT_SEARCH_CONTEXT = URI.create("https://api.nva.unit.no/resources/search");
    public static final String QUERY_PARAMETER = "query";

    @JsonProperty("@context")
    private URI context;
    @JsonProperty("id")
    private URI id;
    @JsonProperty("processingTime")
    private long processingTime;
    @JsonProperty("size")
    private long size;
    private List<JsonNode> hits;

    public SearchResourcesResponse() {
    }

    public static SearchResourcesResponse fromSearchResponse(SearchResponse searchResponse, URI id) {
        List<JsonNode> sourcesList = extractSourcesList(searchResponse);
        long total = searchResponse.getHits().getTotalHits().value;
        long took = searchResponse.getTook().duration();

        return SearchResourcesResponse.builder()
            .withContext(DEFAULT_SEARCH_CONTEXT)
            .withId(id)
            .withHits(sourcesList)
            .withSize(total)
            .withProcessingTime(took)
            .build();
    }

    public static SearchResourcesResponse toSearchResourcesResponse(URI requestUri, String searchTerm, String body) {

        JsonNode values = attempt(() -> objectMapperWithEmpty.readTree(body)).orElseThrow();

        List<JsonNode> sourceList = extractSourceList(values);
        long total = longFromNode(values, TOTAL_JSON_POINTER);
        long took = longFromNode(values, TOOK_JSON_POINTER);
        URI searchResultId = createIdWithQuery(requestUri, searchTerm);
        return SearchResourcesResponse.builder()
            .withContext(DEFAULT_SEARCH_CONTEXT)
            .withId(searchResultId)
            .withProcessingTime(took)
            .withSize(total)
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

    public static Builder builder() {
        return new Builder();
    }

    @JacocoGenerated
    public URI getContext() {
        return context;
    }

    public void setContext(URI context) {
        this.context = context;
    }

    @JacocoGenerated
    public URI getId() {
        return id;
    }

    public void setId(URI id) {
        this.id = id;
    }

    @JacocoGenerated
    public long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }

    @JacocoGenerated
    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @JacocoGenerated
    public List<JsonNode> getHits() {
        return hits;
    }

    public void setHits(List<JsonNode> hits) {
        this.hits = hits;
    }

    @JsonProperty("total")
    @Deprecated(forRemoval = true)
    @JacocoGenerated
    public Long getTotal() {
        return getSize();
    }

    @JsonProperty("total")
    @Deprecated(forRemoval = true)
    @JacocoGenerated
    public void setTotal(long total) {
        //DO NOTHING
    }

    @JsonProperty("took")
    @Deprecated(forRemoval = true)
    @JacocoGenerated
    public Long getTook() {
        return getProcessingTime();
    }

    @JsonProperty("took")
    @Deprecated(forRemoval = true)
    @JacocoGenerated
    public void setTook(long took) {
        //DO NOTHING
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(context, id, processingTime, size, hits);
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
        return processingTime == that.processingTime
               && size == that.size
               && Objects.equals(context, that.context)
               && Objects.equals(id, that.id)
               && Objects.equals(hits, that.hits);
    }

    private static List<JsonNode> extractSourcesList(SearchResponse searchResponse) {
        return Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> hit.getSourceAsMap())
            .map(source -> objectMapperWithEmpty.convertValue(source, JsonNode.class))
            .collect(Collectors.toList());
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

    private static long longFromNode(JsonNode jsonNode, String jsonPointer) {
        JsonNode json = jsonNode.at(jsonPointer);
        return isPopulated(json) ? json.asLong() : 0L;
    }

    private static boolean isPopulated(JsonNode json) {
        return !json.isNull() && !json.asText().isBlank();
    }

    public static final class Builder {

        private final SearchResourcesResponse response;

        public Builder() {
            response = new SearchResourcesResponse();
        }

        public Builder withContext(URI context) {
            response.setContext(context);
            return this;
        }

        public Builder withHits(List<JsonNode> hits) {
            response.setHits(hits);
            return this;
        }

        public Builder withId(URI id) {
            response.setId(id);
            return this;
        }

        public Builder withSize(long size) {
            response.setSize(size);
            return this;
        }

        public Builder withProcessingTime(long processingTime) {
            response.setProcessingTime(processingTime);
            return this;
        }

        public SearchResourcesResponse build() {
            return this.response;
        }
    }
}
