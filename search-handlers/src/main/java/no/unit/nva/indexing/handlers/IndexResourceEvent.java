package no.unit.nva.indexing.handlers;

import java.net.URI;

public class IndexResourceEvent {

    private URI resourceLocation;

    public URI getResourceLocation() {
        return resourceLocation;
    }

    public void setResourceLocation(URI resourceLocation) {
        this.resourceLocation = resourceLocation;
    }
}
