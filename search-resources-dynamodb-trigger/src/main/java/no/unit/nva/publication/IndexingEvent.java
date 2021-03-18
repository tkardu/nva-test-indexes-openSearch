package no.unit.nva.publication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.util.Objects;
import no.unit.nva.model.Publication;
import nva.commons.core.JacocoGenerated;

@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type")
public class IndexingEvent {


    private final Publication oldPublication;
    private final Publication newPublication;


    private final IndexAction action;

    @JsonCreator
    public IndexingEvent(@JsonProperty("oldPublication") Publication oldPublication,
                         @JsonProperty("newPublication") Publication newPublication,
                         @JsonProperty("action") IndexAction action) {
        this.oldPublication = oldPublication;
        this.newPublication = newPublication;
        this.action = action;
    }

    public static IndexingEvent fromDynamoEntryUpdateEvent(IndexAction action, DynamoEntryUpdateEvent event) {
        return new IndexingEvent(event.getOldPublication(), event.getNewPublication(), action);
    }

    @JacocoGenerated
    public Publication getOldPublication() {
        return oldPublication;
    }

    @JacocoGenerated
    public Publication getNewPublication() {
        return newPublication;
    }

    @JacocoGenerated
    public IndexAction getAction() {
        return action;
    }

    @JacocoGenerated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexingEvent)) {
            return false;
        }
        IndexingEvent that = (IndexingEvent) o;
        return Objects.equals(getOldPublication(), that.getOldPublication()) && Objects.equals(
            getNewPublication(), that.getNewPublication()) && getAction() == that.getAction();
    }

    @JacocoGenerated
    @Override
    public int hashCode() {
        return Objects.hash(getOldPublication(), getNewPublication(), getAction());
    }

}
