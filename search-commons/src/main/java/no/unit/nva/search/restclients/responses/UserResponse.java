package no.unit.nva.search.restclients.responses;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperNoEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class UserResponse {

    private ViewingScope viewingScope;

    public ViewingScope getViewingScope() {
        return viewingScope != null ? viewingScope : new ViewingScope();
    }

    public void setViewingScope(ViewingScope viewingScope) {
        this.viewingScope = viewingScope;
    }

    public static UserResponse fromJson(String json) throws JsonProcessingException {
        return objectMapperNoEmpty.readValue(json, UserResponse.class);
    }

    public String toJson() {
        return attempt(() -> objectMapperNoEmpty.writeValueAsString(this)).orElseThrow();
    }

    public static class ViewingScope {

        private Set<URI> includedUnits;
        private Set<URI> excludedUnits;
        private boolean recursive;

        public Set<URI> getIncludedUnits() {
            return includedUnits != null ? includedUnits : Collections.emptySet();
        }

        public void setIncludedUnits(Set<URI> includedUnits) {
            this.includedUnits = includedUnits;
        }

        public Set<URI> getExcludedUnits() {
            return excludedUnits != null ? excludedUnits : Collections.emptySet();

        }

        public void setExcludedUnits(Set<URI> excludedUnits) {
            this.excludedUnits = excludedUnits;
        }

        public boolean isRecursive() {
            return recursive;
        }

        public void setRecursive(boolean recursive) {
            this.recursive = recursive;
        }
    }

}
