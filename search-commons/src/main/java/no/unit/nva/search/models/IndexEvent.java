package no.unit.nva.search.models;

import java.net.URI;
import nva.commons.core.JacocoGenerated;

public class IndexEvent {

    private String eventType;
    private URI uri;

    @JacocoGenerated
    public IndexEvent() {

    }

    @JacocoGenerated
    public String getEventType() {
        return eventType;
    }

    @JacocoGenerated
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @JacocoGenerated
    public URI getUri() {
        return uri;
    }

    @JacocoGenerated
    public void setUri(URI uri) {
        this.uri = uri;
    }
}
