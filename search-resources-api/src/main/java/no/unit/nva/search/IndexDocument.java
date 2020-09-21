package no.unit.nva.search;

import static java.util.Objects.nonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;

public class IndexDocument {

    public static final String PATH_SEPARATOR = "/";
    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    private final String type;
    private final URI id;
    private final List<IndexContributor> contributors;
    private final String title;
    private final IndexDate date;


    /**
     * Creates and IndexDocument with given properties.
     */
    @JacocoGenerated
    @JsonCreator
    public IndexDocument(@JsonProperty("type") String type,
                         @JsonProperty("id") URI id,
                         @JsonProperty("contributors") List<IndexContributor> contributors,
                         @JsonProperty("title") String title,
                         @JsonProperty("date") IndexDate date) {
        this.type = type;
        this.id = id;
        this.contributors = contributors;
        this.title = title;
        this.date = date;
    }

    protected IndexDocument(Builder builder) {
        type = builder.type;
        id = builder.id;
        contributors = builder.contributors;
        title = builder.title;
        date = builder.date;
    }

    public String getType() {
        return type;
    }

    public URI getId() {
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

    @JsonIgnore
    public String getUuidFromId() {
        return getFinalPathElementFromUri();
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

    private String getFinalPathElementFromUri() {
        int lastPathElementSeparator = id.toString().lastIndexOf(PATH_SEPARATOR) + 1;
        return id.toString().substring(lastPathElementSeparator);
    }

    public static final class Builder {

        private String type;
        private URI id;
        private List<IndexContributor> contributors;
        private String title;
        private IndexDate date;

        public Builder() {
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withId(URI id) {
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

        public IndexDocument build() {
            return new IndexDocument(this);
        }

        private boolean isNonNullDate(IndexDate date) {
            return nonNull(date) && date.isPopulated();
        }
    }
}
