package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.core.JacocoGenerated;

import java.net.URI;
import java.util.List;
import java.util.Objects;

@JsonPropertyOrder({"@context", "took","email", "total", "hits" })
public class SearchResourcesResponse {

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

}
