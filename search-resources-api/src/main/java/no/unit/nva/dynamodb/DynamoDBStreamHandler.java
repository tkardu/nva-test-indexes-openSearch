package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.InputException;
import no.unit.nva.search.exception.SearchException;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.SingletonCollector;
import nva.commons.utils.attempt.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.isNull;
import static nva.commons.utils.attempt.Try.attempt;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "202 ACCEPTED";
    public static final String UNKNOWN_OPERATION_ERROR = "Not a known operation";
    public static final String EMPTY_EVENT_NAME_ERROR = "Event name for stream record is empty";
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";
    public static final String IDENTIFIER = "identifier";
    public static final String LOG_MESSAGE_MISSING_EVENT_NAME = "StreamRecord has no event name: ";
    public static final String LOG_ERROR_FOR_INVALID_EVENT_NAME = "Stream record with id {} has invalid event name: {}";
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
    @JacocoGenerated
    public DynamoDBStreamHandler(ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.elasticSearchClient = elasticSearchRestClient;
    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        attempt(() -> processRecordStream(event)).orElseThrow(this::logErrorAndThrowException);
        return SUCCESS_MESSAGE;
    }

    private RuntimeException logErrorAndThrowException(Failure<Void> failure) {
        Exception exception = failure.getException();
        logger.error(ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE, exception);
        throw new RuntimeException(exception);
    }

    private Void processRecordStream(DynamodbEvent event) throws SearchException, InputException {
        for (DynamodbEvent.DynamodbStreamRecord streamRecord : event.getRecords()) {
            processRecord(streamRecord);
        }
        return null;
    }

    private void processRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) throws
            SearchException, InputException {
        validate(streamRecord);
        String eventName = streamRecord.getEventName();

        if (eventName.equals(INSERT) || eventName.equals(MODIFY)) {
            upsertSearchIndex(streamRecord);
        } else if (eventName.equals(REMOVE)) {
            removeFromSearchIndex(streamRecord);
        }
    }

    private void validate(DynamodbStreamRecord streamRecord) throws InputException {
        String eventName = streamRecord.getEventName();
        if (isNull(eventName) || eventName.isBlank()) {
            logger.error(LOG_MESSAGE_MISSING_EVENT_NAME + streamRecord.toString());
            throw new InputException(EMPTY_EVENT_NAME_ERROR);
        }
        if (isNotValidEventName(eventName)) {
            logger.error(LOG_ERROR_FOR_INVALID_EVENT_NAME, streamRecord.getEventID(),
                    streamRecord.getEventName());
            throw new InputException(UNKNOWN_OPERATION_ERROR);
        }
    }

    private boolean isNotValidEventName(String eventName) {
        return !(eventName.equals(INSERT) || eventName.equals(MODIFY) || eventName.equals(REMOVE));
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) throws SearchException {
        logStreamRecord(streamRecord);
        DynamoDBEventTransformer eventTransformer = new DynamoDBEventTransformer();
        IndexDocument document = Optional.ofNullable(eventTransformer.parseStreamRecord(streamRecord)).stream()
                .filter(Objects::nonNull)
                .collect(SingletonCollector.collectOrElse(null));
        if (isNull(document)) {
            return;
        }
        elasticSearchClient.addDocumentToIndex(document);
    }

    private void logStreamRecord(DynamodbStreamRecord streamRecord) {
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        logger.trace("valueMap={}", valueMap.toString());
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
            throws SearchException {
        String identifier = getIdentifierFromStreamRecord(streamRecord);
        elasticSearchClient.removeDocumentFromIndex(identifier);
    }

    private String getIdentifierFromStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
    }
}
