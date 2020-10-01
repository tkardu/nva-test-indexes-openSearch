package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.nonNull;

public class IndexDocument {

    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    private final String type;
    private final UUID id;
    private final List<IndexContributor> contributors;
    private final String title;
    private final IndexDate date;
    @JsonIgnore
    private final String status;


    /**
     * Creates and IndexDocument with given properties.
     */
    @JacocoGenerated
    @JsonCreator
    public IndexDocument(@JsonProperty("type") String type,
                         @JsonProperty("id") UUID id,
                         @JsonProperty("contributors") List<IndexContributor> contributors,
                         @JsonProperty("title") String title,
                         @JsonProperty("date") IndexDate date,
                         @JsonProperty("status") String status) {
        this.type = type;
        this.id = id;
        this.contributors = contributors;
        this.title = title;
        this.date = date;
        this.status = status;
    }

    protected IndexDocument(Builder builder) {
        type = builder.type;
        id = builder.id;
        contributors = builder.contributors;
        title = builder.title;
        date = builder.date;
        status = builder.status;
    }

    public String getType() {
        return type;
    }

    public UUID getId() {
        return id;
    }

    @JacocoGenerated
    public List<IndexContributor> getContributors() {
        return contributors;
    }

    @JacocoGenerated
    public String getTitle() {
        return title;
    }

    @JacocoGenerated
    public IndexDate getDate() {
        return date;
    }


    public String getStatus() {
        return status;
    }

    public String toJsonString() throws JsonProcessingException {
        return mapper.writeValueAsString(this);
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
        return Objects.equals(type, that.type)
            && Objects.equals(id, that.id)
            && Objects.equals(contributors, that.contributors)
            && Objects.equals(title, that.title)
            && Objects.equals(date, that.date);
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(type, id, contributors, title, date);
    }

    public static final class Builder {

        private String type;
        private UUID id;
        private List<IndexContributor> contributors;
        private String title;
        private IndexDate date;
        private String status;

        public Builder() {
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withId(UUID id) {
            this.id = id;
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

        @SuppressWarnings("PMD.NullAssignment")
        public Builder withDate(IndexDate date) {
            this.date = isNonNullDate(date) ? date : null;
            return this;
        }

        public Builder withStatus(String status) {
            this.status = status;
            return this;
        }

        public IndexDocument build() {
            return new IndexDocument(this);
        }

        private boolean isNonNullDate(IndexDate date) {
            return nonNull(date) && date.isPopulated();
        }
    }
}
