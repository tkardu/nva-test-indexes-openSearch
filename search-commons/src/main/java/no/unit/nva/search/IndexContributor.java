package no.unit.nva.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import no.unit.nva.model.Contributor;
import no.unit.nva.model.Identity;
import nva.commons.core.JacocoGenerated;

public class IndexContributor {

    private final URI id;
    private final String name;

    @JsonCreator
    public IndexContributor(@JsonProperty("id") URI id,
                            @JsonProperty("name") String name) {
        this.id = id;
        this.name = name;
    }

    private IndexContributor(Builder builder) {
        id = builder.id;
        name = builder.name;
    }

    public static IndexContributor fromContributor(Contributor contributor) {
        Optional<Identity> identity = Optional.of(contributor).map(Contributor::getIdentity);
        URI contributorId = identity.map(Identity::getId).orElse(null);
        String name = identity.map(Identity::getName).orElse(null);
        return new IndexContributor(contributorId, name);
    }

    public URI getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName());
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

    public static final class Builder {

        private URI id;
        private String name;

        public Builder() {
        }

        public Builder withId(URI id) {
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
}
