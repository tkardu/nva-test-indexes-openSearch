package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;

import java.util.List;
import java.util.Objects;

public class IndexDocument {
    private final String type;
    private final String id;
    private final List<IndexContributor> contributors;
    private final String title;
    private final String date;
    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    /**
     * Creates and IndexDocument with given properties.
     */
    @JacocoGenerated
    @JsonCreator
    public IndexDocument(@JsonProperty("type") String type,
                         @JsonProperty("id") String id,
                         @JsonProperty("contributors") List<IndexContributor> contributors,
                         @JsonProperty("title") String title,
                         @JsonProperty("date") String date) {
        this.type = type;
        this.id = id;
        this.contributors = contributors;
        this.title = title;
        this.date = date;
    }

    private IndexDocument(Builder builder) {
        type = builder.type;
        id = builder.id;
        contributors = builder.contributors;
        title = builder.title;
        date = builder.date;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public List<IndexContributor> getContributors() {
        return contributors;
    }

    public String getTitle() {
        return title;
    }

    public String getDate() {
        return date;
    }

    public String toJsonString() throws JsonProcessingException {
        return mapper.writeValueAsString(this);
    }

    public static final class Builder {
        private String type;
        private String id;
        private List<IndexContributor> contributors;
        private String title;
        private String date;

        public Builder() {
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withId(String identifier) {
            this.id = identifier;
            return this;
        }

        public Builder withContributors(List<IndexContributor> contributors) {
            this.contributors = contributors;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withDate(String date) {
            this.date = date;
            return this;
        }

        public IndexDocument build() {
            return new IndexDocument(this);
        }
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexDocument)) {
            return false;
        }
        IndexDocument that = (IndexDocument) o;
        return Objects.equals(getType(), that.getType())
                && Objects.equals(getId(), that.getId())
                && Objects.equals(getContributors(), that.getContributors())
                && Objects.equals(getTitle(), that.getTitle())
                && Objects.equals(getDate(), that.getDate());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getType(), getId(), getContributors(), getTitle(), getDate());
    }
}
