package no.unit.nva.publication;

import com.amazonaws.services.lambda.runtime.Context;
import java.util.Set;
import no.unit.nva.events.handlers.EventHandler;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"PMD.UnusedPrivateField","PMD.SingularField"})
public class DynamoDBStreamHandler extends EventHandler<DynamoEntryUpdateEvent, String> {

    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "202 ACCEPTED";
    public static final String UNKNOWN_OPERATION_ERROR = "Not a known operation";
    public static final String EMPTY_EVENT_NAME_ERROR = "Event name for stream record is empty";
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";
    public static final Set<String> UPSERT_EVENTS = Set.of(INSERT, MODIFY);
    public static final Set<String> REMOVE_EVENTS = Set.of(REMOVE);
    public static final String IDENTIFIER = "identifier";
    public static final String LOG_MESSAGE_MISSING_EVENT_NAME = "StreamRecord has no event name: ";
    public static final String LOG_ERROR_FOR_INVALID_EVENT_NAME = "Stream record with id {} has invalid event name: {}";
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    public static final String MISSING_PUBLICATION_STATUS =
            "The data from DynamoDB was incomplete, missing required field status on id: {}, ignoring entry";
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
    protected String processInput(DynamoEntryUpdateEvent input, AwsEventBridgeEvent<DynamoEntryUpdateEvent> event,
                                  Context context) {

        return null;

    }


}
