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
import nva.commons.utils.attempt.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.isNull;
import static no.unit.nva.dynamodb.IndexDocumentGenerator.PUBLISHED;
import static no.unit.nva.dynamodb.IndexDocumentGenerator.STATUS;
import static nva.commons.utils.attempt.Try.attempt;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

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

    private void processRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) throws SearchException, InputException {

        Optional<String> eventName = Optional.ofNullable(streamRecord.getEventName())
                .map(String::toUpperCase)
                .filter(name -> !name.isBlank());

        if (eventName.isPresent()) {
            executeIndexEvent(streamRecord, eventName.get());
        } else {
            logEmptyEventNameThrowInputException(streamRecord);
        }
    }

    private boolean isNotPublished(DynamodbStreamRecord streamRecord) {
        AttributeValue status = streamRecord.getDynamodb().getNewImage().get(STATUS);
        if (isNull(status) || isNull(status.getS())) {
            logEmptyPublicationStatus(streamRecord);
            return true;
        }
        return !status.getS().equalsIgnoreCase(PUBLISHED);
    }

    private void logEmptyPublicationStatus(DynamodbStreamRecord streamRecord) {
        String identifier = streamRecord.getDynamodb().getNewImage().get(IDENTIFIER).getS();
        logger.warn(MISSING_PUBLICATION_STATUS, identifier);
    }

    private void executeIndexEvent(DynamodbStreamRecord streamRecord, String eventName) throws SearchException,
                                                                                               InputException {
        if (UPSERT_EVENTS.contains(eventName) && isNotPublished(streamRecord)) {
            return;
        }
        if (UPSERT_EVENTS.contains(eventName)) {
            upsertSearchIndex(streamRecord);
        } else if (REMOVE_EVENTS.contains(eventName)) {
            removeFromSearchIndex(streamRecord);
        } else {
            logInvalidEventNameThrowInputException(streamRecord);
        }
    }

    private void logEmptyEventNameThrowInputException(DynamodbStreamRecord streamRecord) throws InputException {
        logger.error(LOG_MESSAGE_MISSING_EVENT_NAME + streamRecord.toString());
        throw new InputException(EMPTY_EVENT_NAME_ERROR);
    }

    private void logInvalidEventNameThrowInputException(DynamodbStreamRecord streamRecord) throws InputException {
        logger.error(LOG_ERROR_FOR_INVALID_EVENT_NAME, streamRecord.getEventID(),
            streamRecord.getEventName());
        throw new InputException(UNKNOWN_OPERATION_ERROR);
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) throws SearchException {
        logStreamRecord(streamRecord);
        IndexDocument document = IndexDocumentGenerator.fromStreamRecord(streamRecord);
        elasticSearchClient.addDocumentToIndex(document);
    }

    private void logStreamRecord(DynamodbStreamRecord streamRecord) {
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        logger.trace("valueMap={}", valueMap.toString());
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
        throws SearchException {
        elasticSearchClient.removeDocumentFromIndex(getIdentifierFromStreamRecord(streamRecord));
    }

    private String getIdentifierFromStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
    }
}
