package no.unit.nva.publication;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.events.handlers.DestinationsEventBridgeEventHandler;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.model.Publication;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.attempt.Failure;
import nva.commons.core.exceptions.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;
import static no.unit.nva.model.PublicationStatus.PUBLISHED;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperNoEmpty;
import static nva.commons.core.attempt.Try.attempt;

public class PublicationUpdateEventHandler
    extends DestinationsEventBridgeEventHandler<DynamoEntryUpdateEvent, Void> {

    public static final String INVALID_EVENT_ERROR = "Invalid event: ";
    public static final String UNKNOWN_OPERATION_ERROR = "Unknown operation: ";
    public static final String RESOURCE_IS_NOT_PUBLISHED_WARNING = "Resource is not published: ";
    public static final String REMOVING_RESOURCE_WARNING = "Deleting resource: ";
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";
    public static final Set<String> UPSERT_EVENTS = Set.of(INSERT, MODIFY);
    public static final Set<String> REMOVE_EVENTS = Set.of(REMOVE);
    public static final Set<String> VALID_EVENTS = validEvents();
    public static final String NO_TITLE_WARNING = "Resource has no title: ";
    public static final String NO_TYPE_WARNING = "Resource has no publication type: ";
    private static final Logger logger = LoggerFactory.getLogger(PublicationUpdateEventHandler.class);
    private final ElasticSearchHighLevelRestClient elasticSearchClient;

    @JacocoGenerated
    public PublicationUpdateEventHandler() {
        this(new ElasticSearchHighLevelRestClient());
    }

    /**
     * Constructor for PublicationUpdateEventHandler for testing.
     *
     * @param elasticSearchRestClient elasticSearchRestClient to be injected for testing
     */
    public PublicationUpdateEventHandler(ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        super(DynamoEntryUpdateEvent.class);
        this.elasticSearchClient = elasticSearchRestClient;
    }

    @Override
    protected Void processInputPayload(DynamoEntryUpdateEvent input,
                                       AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event,
                                       Context context) {

        return attempt(() -> validateEvent(input, event))
            .map(this::processEvent)
            .orElse(this::logFailure);
    }

    private Void logFailure(Failure<Void> fail) {
        logger.debug(ExceptionUtils.stackTraceInSingleLine(fail.getException()));
        return null;
    }

    private static Set<String> validEvents() {
        Set<String> events = new HashSet<>(UPSERT_EVENTS);
        events.addAll(REMOVE_EVENTS);
        return events;
    }

    private Void processEvent(DynamoEntryUpdateEvent input) throws SearchException {
        if (isDeleteEvent(input)) {
            removeEntry(input);
        } else if (isUpdateEvent(input) && resourceIsPublished(input)) {
            IndexDocument indexDocument = IndexDocument.fromPublication(input.getNewPublication());
            indexDocument(indexDocument);
        }
        return null;
    }

    private Void indexDocument(IndexDocument indexDocument)
        throws SearchException {
        if (indexDocumentShouldBePublished(indexDocument)) {
            elasticSearchClient.addDocumentToIndex(indexDocument);
        }
        return null;
    }

    private boolean indexDocumentShouldBePublished(IndexDocument indexDocument) {
        return Stream.of(indexDocument)
            .filter(IndexDocument::hasPublicationType)
            .anyMatch(IndexDocument::hasTitle);
    }

    private Void removeEntry(DynamoEntryUpdateEvent input) throws SearchException {
        logger.warn(REMOVING_RESOURCE_WARNING + input.getOldPublication().getIdentifier());
        elasticSearchClient.removeDocumentFromIndex(input.getOldPublication().getIdentifier().toString());
        return null;
    }

    private boolean resourceIsPublished(DynamoEntryUpdateEvent input) {
        Publication newPublication = input.getNewPublication();
        if (!PUBLISHED.equals(newPublication.getStatus())) {
            logger.warn(RESOURCE_IS_NOT_PUBLISHED_WARNING + newPublication.getIdentifier());
            return false;
        }
        return true;
    }

    private boolean isUpdateEvent(DynamoEntryUpdateEvent input) {
        return isPresent(input.getNewPublication());
    }

    private DynamoEntryUpdateEvent validateEvent(DynamoEntryUpdateEvent updateEvent,
                AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event) {
        if (notPresent(updateEvent.getNewPublication()) && notPresent(updateEvent.getOldPublication())) {
            throw new IllegalArgumentException(INVALID_EVENT_ERROR + serializeEvent(event));
        }
        if (!VALID_EVENTS.contains(updateEvent.getUpdateType())) {
            throw new IllegalArgumentException(UNKNOWN_OPERATION_ERROR + updateEvent.getUpdateType());
        }
        return updateEvent;
    }

    private String serializeEvent(AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event) {
        return attempt(() -> objectMapperNoEmpty.writeValueAsString(event)).orElseThrow();
    }

    private boolean isDeleteEvent(DynamoEntryUpdateEvent input) {
        return resourceIsActuallyDeleted(input) || resourceIsUnpublished(input);
    }

    private boolean resourceIsUnpublished(DynamoEntryUpdateEvent input) {
        return isPublished(input.getOldPublication()) && !isPublished(input.getNewPublication());
    }

    private boolean isPublished(Publication publication) {
        return Optional.ofNullable(publication)
            .map(Publication::getStatus)
            .map(PUBLISHED::equals)
            .orElse(false);
    }

    private boolean resourceIsActuallyDeleted(DynamoEntryUpdateEvent input) {
        return isPresent(input.getOldPublication()) && notPresent(input.getNewPublication());
    }

    private boolean notPresent(Publication publication) {
        return !isPresent(publication);
    }

    private boolean isPresent(Publication publication) {
        return nonNull(publication) && nonNull(publication.getIdentifier());
    }
}
