package no.unit.nva.publication;

import static java.util.Objects.nonNull;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import no.unit.nva.events.handlers.DestinationsEventBridgeEventHandler;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.model.Publication;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"PMD.UnusedPrivateField", "PMD.SingularField"})
public class DynamoDBStreamHandler extends DestinationsEventBridgeEventHandler<DynamoEntryUpdateEvent, String> {

    public static final String SUCCESS_MESSAGE = "202 ACCEPTED";
    public static final String INVALID_EVENT_ERROR = "Invalid event: ";
    public static final String UNKNOWN_OPERATION_ERROR = "Unknown operation: ";
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";
    public static final Set<String> UPSERT_EVENTS = Set.of(INSERT, MODIFY);
    public static final Set<String> REMOVE_EVENTS = Set.of(REMOVE);
    public static final Set<String> VALID_EVENTS = validEvents();
    public static final boolean NEW_IMAGE_DOES_NOT_CONTAIN_PUBLISHED_RESOURCE = false;

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    private final ElasticSearchHighLevelRestClient elasticSearchClient;

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {
        this(new Environment());
    }

    /**
     * constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler(Environment environment) {
        this(new ElasticSearchHighLevelRestClient(environment));
    }

    /**
     * Constructor for DynamoDBStreamHandler for testing.
     *
     * @param elasticSearchRestClient elasticSearchRestClient to be injected for testing
     */
    public DynamoDBStreamHandler(ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        super(DynamoEntryUpdateEvent.class);
        this.elasticSearchClient = elasticSearchRestClient;
    }

    @Override
    protected String processInputPayload(DynamoEntryUpdateEvent input,
                                         AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event,
                                         Context context) {

        validateEvent(input, event);

        attempt(() -> processEvent(input)).orElseThrow();

        return SUCCESS_MESSAGE;
    }

    private static Set<String> validEvents() {
        Set<String> events = new HashSet<>(UPSERT_EVENTS);
        events.addAll(REMOVE_EVENTS);
        return events;
    }

    private Void processEvent(DynamoEntryUpdateEvent input) throws SearchException {
        if (isDeleteEvent(input)) {
            removeEntry(input);
        } else if (isUpdateEvent(input) && resourceShouldBeIndexed(input)) {
            IndexDocument indexDocument = IndexDocument.fromPublication(input.getNewPublication());
            elasticSearchClient.addDocumentToIndex(indexDocument);
        }
        return null;
    }

    private void removeEntry(DynamoEntryUpdateEvent input) throws SearchException {
        elasticSearchClient.removeDocumentFromIndex(input.getOldPublication().getIdentifier().toString());
    }

    private boolean resourceShouldBeIndexed(DynamoEntryUpdateEvent input) {
        return Optional.of(input)
                   .map(DynamoEntryUpdateEvent::getNewPublication)
                   .map(Publication::getStatus)
                   .map(PublicationStatus.PUBLISHED::equals)
                   .orElse(NEW_IMAGE_DOES_NOT_CONTAIN_PUBLISHED_RESOURCE);
    }

    private boolean isUpdateEvent(DynamoEntryUpdateEvent input) {
        return isPresent(input.getNewPublication());
    }

    private void validateEvent(DynamoEntryUpdateEvent updateEvent,
                               AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event) {
        if (notPresent(updateEvent.getNewPublication()) && notPresent(updateEvent.getOldPublication())) {
            throw new IllegalArgumentException(INVALID_EVENT_ERROR + serializeEvent(event));
        }
        if (!VALID_EVENTS.contains(updateEvent.getUpdateType())) {
            throw new IllegalArgumentException(UNKNOWN_OPERATION_ERROR + updateEvent.getUpdateType());
        }

    }

    private String serializeEvent(AwsEventBridgeEvent<AwsEventBridgeDetail<DynamoEntryUpdateEvent>> event) {
       return attempt(() -> JsonUtils.objectMapperNoEmpty.writeValueAsString(event)).orElseThrow();
    }

    private boolean isDeleteEvent(DynamoEntryUpdateEvent input) {
        return isPresent(input.getOldPublication()) && notPresent(input.getNewPublication());
    }

    private boolean notPresent(Publication publication) {
        return !isPresent(publication);
    }

    private boolean isPresent(Publication publication) {
        return nonNull(publication) && nonNull(publication.getIdentifier());
    }
}
