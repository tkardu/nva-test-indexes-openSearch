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
    private final String identifier;
    private final List<IndexContributor> contributors;
    private final String title;
    private final String date;
    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    @JacocoGenerated
    @JsonCreator
    public IndexDocument(@JsonProperty("type") String type,
                         @JsonProperty("identifier") String identifier,
                         @JsonProperty("contributors") List<IndexContributor> contributors,
                         @JsonProperty("title") String title,
                         @JsonProperty("date") String date) {
        this.type = type;
        this.identifier = identifier;
        this.contributors = contributors;
        this.title = title;
        this.date = date;
    }

    private IndexDocument(Builder builder) {
        type = builder.type;
        identifier = builder.identifier;
        contributors = builder.contributors;
        title = builder.title;
        date = builder.date;
    }

    public String getType() {
        return type;
    }

    public String getIdentifier() {
        return identifier;
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
        private String identifier;
        private List<IndexContributor> contributors;
        private String title;
        private String date;

        public Builder() {
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withIdentifier(String identifier) {
            this.identifier = identifier;
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
                && Objects.equals(getIdentifier(), that.getIdentifier())
                && Objects.equals(getContributors(), that.getContributors())
                && Objects.equals(getTitle(), that.getTitle())
                && Objects.equals(getDate(), that.getDate());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getType(), getIdentifier(), getContributors(), getTitle(), getDate());
    }
}
