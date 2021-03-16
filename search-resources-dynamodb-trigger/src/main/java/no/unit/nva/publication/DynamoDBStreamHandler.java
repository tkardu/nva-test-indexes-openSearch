package no.unit.nva.publication;

import static java.util.Objects.nonNull;
import static nva.commons.core.attempt.Try.attempt;
import com.amazonaws.services.lambda.runtime.Context;
import java.util.HashSet;
import java.util.Set;
import no.unit.nva.events.handlers.DestinationsEventBridgeEventHandler;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.model.Publication;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"PMD.UnusedPrivateField", "PMD.SingularField"})
public class DynamoDBStreamHandler extends DestinationsEventBridgeEventHandler<DynamoEntryUpdateEvent, String> {

    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "202 ACCEPTED";
    public static final String UNKNOWN_OPERATION_ERROR = "Unknown operation: ";
    public static final String EMPTY_EVENT_NAME_ERROR = "Event name for stream record is empty";
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";
    public static final Set<String> UPSERT_EVENTS = Set.of(INSERT, MODIFY);
    public static final Set<String> REMOVE_EVENTS = Set.of(REMOVE);
    public static final Set<String> VALID_EVENTS = validEvents();
    public static final String IDENTIFIER = "identifier";
    public static final String LOG_MESSAGE_MISSING_EVENT_NAME = "StreamRecord has no event name: ";
    public static final String LOG_ERROR_FOR_INVALID_EVENT_NAME = "Stream record with id {} has invalid event name: {}";
    public static final String MISSING_PUBLICATION_STATUS =
        "The data from DynamoDB was incomplete, missing required field status on id: {}, ignoring entry";
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
        validateEvent(input.getUpdateType());

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
            elasticSearchClient.removeDocumentFromIndex(input.getOldPublication().getIdentifier().toString());
        }
        return null;
    }

    private void validateEvent(String updateType) {
        if (!VALID_EVENTS.contains(updateType)) {
            throw new IllegalArgumentException(UNKNOWN_OPERATION_ERROR + updateType);
        }
    }

    private boolean isDeleteEvent(DynamoEntryUpdateEvent input) {
        return isPresent(input.getOldPublication()) && notPresent(input.getNewPublication());
    }

    private boolean notPresent(Publication publication) {
        return !isPresent(publication);
    }

    private boolean isPresent(Publication publication) {
        return nonNull(publication) && nonEmptyPublication(publication);
    }

    private boolean nonEmptyPublication(Publication publication) {
        return nonNull(publication.getIdentifier());
    }
}
