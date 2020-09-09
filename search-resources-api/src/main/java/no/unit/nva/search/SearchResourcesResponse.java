package no.unit.nva.search;

import nva.commons.utils.JacocoGenerated;

import java.util.List;

@JacocoGenerated
public class SearchResourcesResponse {

    private final List<String> hits;

    private SearchResourcesResponse(Builder builder) {
        this.hits = builder.hits;
    }

    public static final class Builder {
        private List<String> hits;

        public Builder() {
        }

        public Builder withHits(List<String> hits) {
            this.hits = hits;
            return this;
        }

        public SearchResourcesResponse build() {
            return new SearchResourcesResponse(this);
        }
    }

}
