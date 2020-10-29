package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
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

    private final String publicationType;
    private final UUID id;
    private final List<IndexContributor> contributors;
    private final String mainTitle;
    private final String publicationAbstract;
    private final String description;
    private final String owner;
    private final IndexDate publishedDate;


    /**
     * Creates and IndexDocument with given properties.
     */
    @JacocoGenerated
    @JsonCreator
    public IndexDocument(@JsonProperty("publicationType") String publicationType,
                         @JsonProperty("id") UUID id,
                         @JsonProperty("contributors") List<IndexContributor> contributors,
                         @JsonProperty("mainTitle") String mainTitle,
                         @JsonProperty("abstract") String publicationAbstract,
                         @JsonProperty("description") String description,
                         @JsonProperty("owner") String owner,
                         @JsonProperty("publishedDate") IndexDate publishedDate) {
        this.publicationType = publicationType;
        this.id = id;
        this.contributors = contributors;
        this.mainTitle = mainTitle;
        this.publishedDate = publishedDate;
        this.description = description;
        this.publicationAbstract = publicationAbstract;
        this.owner = owner;

    }

    protected IndexDocument(Builder builder) {
        publicationType = builder.publicationType;
        id = builder.id;
        contributors = builder.contributors;
        mainTitle = builder.mainTitle;
        description = builder.description;
        owner = builder.owner;
        publishedDate = builder.publishedDate;
        publicationAbstract = builder.publicationAbstract;

    }

    public String getPublicationType() {
        return publicationType;
    }

    public UUID getId() {
        return id;
    }

    @JacocoGenerated
    public List<IndexContributor> getContributors() {
        return contributors;
    }

    @JacocoGenerated
    public String getMainTitle() {
        return mainTitle;
    }

    @JacocoGenerated
    public IndexDate getPublishedDate() {
        return publishedDate;
    }

    @JacocoGenerated
    public String getAbstract() {
        return publicationAbstract;
    }

    @JacocoGenerated
    public String getDescription() {
        return description;
    }

    @JacocoGenerated
    public String getOwner() {
        return owner;
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
        return Objects.equals(publicationType, that.publicationType)
            && Objects.equals(id, that.id)
            && Objects.equals(contributors, that.contributors)
            && Objects.equals(mainTitle, that.mainTitle)
            && Objects.equals(owner, that.owner)
            && Objects.equals(description, that.description)
            && Objects.equals(publicationAbstract, that.publicationAbstract)
            && Objects.equals(publishedDate, that.publishedDate);
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(publicationType,
                id,
                contributors,
                mainTitle,
                publishedDate,
                owner,
                description,
                publicationAbstract);
    }

    public static final class Builder {

        private String publicationType;
        private UUID id;
        private List<IndexContributor> contributors;
        private IndexDate publishedDate;
        private String mainTitle;
        private String publicationAbstract;
        private String description;
        private String owner;

        public Builder() {
        }

        public Builder withType(String type) {
            this.publicationType = type;
            return this;
        }

        public Builder withId(UUID id) {
            this.id = id;
            return this;
        }

        public Builder withOwner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder withContributors(List<IndexContributor> contributors) {
            this.contributors = contributors;
            return this;
        }

        public Builder withMainTitle(String mainTitle) {
            this.mainTitle = mainTitle;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withAbstract(String publicationAbstract) {
            this.publicationAbstract = publicationAbstract;
            return this;
        }

        @SuppressWarnings("PMD.NullAssignment")
        public Builder withPublishedDate(IndexDate date) {
            this.publishedDate = isNonNullDate(date) ? date : null;
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
