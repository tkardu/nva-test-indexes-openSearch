package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import nva.commons.utils.JacocoGenerated;

import java.util.Objects;

public class IndexContributor {
    private final String identifier;
    private final String name;

    @JacocoGenerated
    @JsonCreator
    public IndexContributor(@JsonProperty("identifier") String identifier,
                            @JsonProperty("name") String name) {
        this.identifier = identifier;
        this.name = name;
    }

    private IndexContributor(Builder builder) {
        identifier = builder.identifier;
        name = builder.name;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getName() {
        return name;
    }

    public static final class Builder {
        private String identifier;
        private String name;

        public Builder() {
        }

        public Builder withIdentifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public IndexContributor build() {
            return new IndexContributor(this);
        }
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexContributor)) {
            return false;
        }
        IndexContributor that = (IndexContributor) o;
        return Objects.equals(getIdentifier(), that.getIdentifier())
                && Objects.equals(getName(), that.getName());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier(), getName());
    }
}
