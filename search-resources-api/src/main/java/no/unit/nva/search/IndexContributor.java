package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import nva.commons.utils.JacocoGenerated;

import java.util.Objects;

public class IndexContributor {
    private final String id;
    private final String name;

    @JacocoGenerated
    @JsonCreator
    public IndexContributor(@JsonProperty("id") String id,
                            @JsonProperty("name") String name) {
        this.id = id;
        this.name = name;
    }

    private IndexContributor(Builder builder) {
        id = builder.id;
        name = builder.name;
    }

    @JacocoGenerated
    public String getId() {
        return id;
    }

    @JacocoGenerated
    public String getName() {
        return name;
    }

    public static final class Builder {
        private String id;
        private String name;

        public Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
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
        return Objects.equals(getId(), that.getId())
                && Objects.equals(getName(), that.getName());
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName());
    }
}
