package no.unit.nva.search.restclients.responses;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

public class ViewingScope {

    private Set<URI> includedUnits;
    private Set<URI> excludedUnits;
    private boolean recursive;

    public static ViewingScope create(URI... includedUnits) {
        var viewingScope = new ViewingScope();
        viewingScope.setIncludedUnits(Set.of(includedUnits));
        return viewingScope;
    }

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
