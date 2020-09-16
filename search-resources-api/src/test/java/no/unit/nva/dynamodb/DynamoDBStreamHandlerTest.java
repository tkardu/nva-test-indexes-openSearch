package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.unit.nva.model.Contributor;
import no.unit.nva.model.Identity;
import no.unit.nva.model.exceptions.MalformedContributorException;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.InputException;
import nva.commons.utils.Environment;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;
import nva.commons.utils.log.LogUtils;
import nva.commons.utils.log.TestAppender;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static nva.commons.utils.Environment.ENVIRONMENT_VARIABLE_NOT_SET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamoDBStreamHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String EVENT_JSON_STRING_NAME = "s";
    public static final String CONTRIBUTOR_SEQUENCE_POINTER = "/m/sequence";
    public static final String CONTRIBUTOR_NAME_POINTER = "/m/identity/m/name";
    public static final String CONTRIBUTOR_ARPID_POINTER = "/m/identity/m/arpId";
    public static final String CONTRIBUTOR_ID_POINTER = "/m/identity/m/id";
    public static final String CONTRIBUTOR_POINTER = "/records/0/dynamodb/newImage/entityDescription/m/contributors";
    public static final String EVENT_JSON_LIST_NAME = "l";
    public static final String CONTRIBUTOR_TEMPLATE_JSON = "contributorTemplate.json";
    public static final String EVENT_TEMPLATE_JSON = "eventTemplate.json";
    public static final String ENTITY_DESCRIPTION_MAIN_TITLE_POINTER =
        "/records/0/dynamodb/newImage/entityDescription/m/mainTitle";
    public static final String PUBLICATION_INSTANCE_TYPE_POINTER =
        "/records/0/dynamodb/newImage/entityDescription/m/reference/m/publicationInstance/m/type";
    public static final String FIRST_RECORD_POINTER = "/records/0";
    public static final String EVENT_NAME = "eventName";
    public static final String MODIFY = "MODIFY";
    public static final String IMAGE_IDENTIFIER_JSON_POINTER = "/records/0/dynamodb/newImage/identifier";
    public static final String DATE_SEPARATOR = "-";
    public static final int YEAR_INDEX = 0;
    public static final int MONTH_INDEX = 1;
    public static final int DAY_INDEX = 2;
    public static final String ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER =
        "/records/0/dynamodb/newImage/entityDescription/m/date/m";
    public static final String EVENT_YEAR_NAME = "year";
    public static final String EVENT_MONTH_NAME = "month";
    public static final String EVENT_DAY_NAME = "day";
    public static final String EXAMPLE_URI_BASE = "https://example.org/wanderlust/";
    public static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String UNKNOWN_EVENT = "UnknownEvent";
    private static final String SAMPLE_MODIFY_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
    private static final String SAMPLE_REMOVE_EVENT_FILENAME = "DynamoDBStreamRemoveEvent.json";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    public static final String EVENT_ID = "eventID";
    private static final String UNKNOWN_OPERATION_ERROR = "Not a known operation";
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private DynamoDBStreamHandler handler;
    private Context context;
    private Environment environment;

    private JsonNode contributorTemplate;
    private TestAppender testAppender;
    public static final String EXPECTED_MESSAGE = "expectedMessage";

    /**
     * Set up test environment.
     *
     * @throws IOException          some error occurred
     */
    @BeforeEach
    public void init() throws IOException {
        context = mock(Context.class);
        environment = setupMockEnvironment();

        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        when(restHighLevelClient.search(any(), any())).thenReturn(searchResponse);
        when(restHighLevelClient.delete(any(), any())).thenReturn(deleteResponse);

        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment,
                restHighLevelClient);

        testAppender = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
        contributorTemplate = mapper.readTree(IoUtils.inputStreamFromResources(Paths.get(CONTRIBUTOR_TEMPLATE_JSON)));
    }

    @Test
    @DisplayName("testCreateHandlerWithEmptyEnvironmentShouldFail")
    public void testCreateHandlerWithEmptyEnvironmentShouldFail() {
        Exception exception = assertThrows(IllegalStateException.class, DynamoDBStreamHandler::new);
        assertThat(exception.getMessage(),containsString(ENVIRONMENT_VARIABLE_NOT_SET));
    }

    @Test
    @DisplayName("testHandlerHandleSimpleRemoveEventWithoutProblem")
    public void testHandlerHandleSimpleRemoveEventWithoutProblem() throws IOException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_REMOVE_EVENT_FILENAME);
        String response = handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }

    @Test
    public void handleRequestThrowsExceptionWhenInputIsHasUnknownEventName() throws IOException {


        String expectedEventId = "12345";
        DynamodbEvent requestWithUnknownEventName = generateEventWithUnknownEventNameAndSomeEventId(expectedEventId);
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> handler.handleRequest(requestWithUnknownEventName, context));


        InputException cause = (InputException) exception.getCause();
        assertThat(cause.getMessage(), containsString(UNKNOWN_OPERATION_ERROR));

        assertThat(testAppender.getMessages(), containsString(UNKNOWN_EVENT));
        assertThat(testAppender.getMessages(), containsString(expectedEventId));
    }

    @Test
    public void handleRequestThrowsExceptionWhenInputIsHasNoEventName() throws IOException {

        DynamodbEvent requestWithUnknownEventName = generateEventWithoutEventName();
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> handler.handleRequest(requestWithUnknownEventName, context));


        InputException cause = (InputException) exception.getCause();
        assertThat(cause.getMessage(), containsString(DynamoDBStreamHandler.EMPTY_EVENT_NAME_ERROR));

        assertThat(testAppender.getMessages(), containsString(DynamoDBStreamHandler.LOG_MESSAGE_MISSING_EVENT_NAME));
    }

    @Test
    @DisplayName("testHandleExceptionInEventHandlingShouldGiveException")
    public void testHandleExceptionInEventHandlingShouldGiveException() throws IOException {


        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        Exception searchException = new RuntimeException(EXPECTED_MESSAGE);
        when(restHighLevelClient.update(any(), any())).thenThrow(searchException);
        when(restHighLevelClient.search(any(), any())).thenThrow(searchException);
        when(restHighLevelClient.delete(any(), any())).thenThrow(searchException);

        ElasticSearchHighLevelRestClient elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment,
                restHighLevelClient);

        handler = new DynamoDBStreamHandler(elasticSearchRestClient);

        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        Executable actionThrowingException = () -> handler.handleRequest(requestEvent, context);
        assertThrows(RuntimeException.class, actionThrowingException);
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, single author")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithContributorsWhenInputIsModifyEvent()
        throws IOException,InterruptedException {
        String identifier = "1006a";
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
            generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "Book";
        String date = "2020-09-08";

        DynamodbEvent requestEvent = generateRequestEvent(MODIFY, identifier, type, mainTitle, contributors, date);
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent);

        IndexDocument expected = generateIndexDocument(identifier, contributors, mainTitle, type, date);
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, multiple authors")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenInputIsModifyEvent()
        throws IOException, InterruptedException {
        String identifier = "1006a";
        String firstContributorIdentifier = "123";
        String firstContributorName = "Bólsön Kölàdỳ";
        String secondContributorIdentifier = "345";
        String secondContributorName = "Mèrdok Hüber";
        List<Contributor> contributors = new ArrayList<>();
        contributors.add(generateContributor(firstContributorIdentifier, firstContributorName, 1));
        contributors.add(generateContributor(secondContributorIdentifier, secondContributorName, 2));
        String mainTitle = "Moi buki";
        String type = "Book";
        String date = "2020-09-08";

        DynamodbEvent requestEvent = generateRequestEvent(MODIFY, identifier, type, mainTitle, contributors, date);
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent);

        IndexDocument expected = generateIndexDocument(identifier, contributors, mainTitle, type, date);
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with empty record, year only")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithYearOnlyWhenInputIsModifyEvent()
            throws IOException {
        String date = "2020";

        DynamodbEvent requestEvent = generateRequestEvent(MODIFY, null, null, null, null, date);
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent);

        IndexDocument expected = generateIndexDocument(null, Collections.emptyList(), null, null, date);
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with empty record, year and month only")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithYearAndMonthOnlyWhenInputIsModifyEvent()
        throws IOException {
        String date = "2020-09";

        DynamodbEvent requestEvent = generateRequestEvent(MODIFY,
                null,
                null,
                null,
                null,
                date);
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent);

        IndexDocument expected = generateIndexDocument(null,
                Collections.emptyList(),
                null,
                null,
                date);
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with empty record")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentNullValuesWhenInputIsModifyEvent() throws
                                                                                                           IOException {
        DynamodbEvent requestEvent = generateRequestEvent(MODIFY,
                null,
                null,
                null,
                null,
                null);
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent);
        IndexDocument expected = generateIndexDocument(null,
                Collections.emptyList(),
                null,
                null,
                null);
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
        assertThat(actual, samePropertyValuesAs(expected));
    }


    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        return environment;
    }

    private DynamodbEvent generateEventWithUnknownEventNameAndSomeEventId(String eventId) throws IOException {
        return generateEventWithEventId(eventId,
                UNKNOWN_EVENT,
                null,
                null,
                null,
                Collections.emptyList(),
                null);
    }

    private DynamodbEvent generateEventWithEventId(String eventId,
                                                   String eventName,
                                                   String identifier,
                                                   String type,
                                                   String mainTitle,
                                                   List<Contributor> contributors,
                                                   String date) throws IOException {
        ObjectNode event = getEventTemplate();
        updateEventIdentifier(eventId,event);
        updateEventImageIdentifier(identifier, event);
        updateEventName(eventName, event);
        updateReferenceType(type, event);
        updateEntityDescriptionMainTitle(mainTitle, event);
        updateEntityDescriptionContributors(contributors, event);
        updateDate(date, event);
        return toDynamodbEvent(event);


    }

    private void updateEventIdentifier(String eventId, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, FIRST_RECORD_POINTER, EVENT_ID,eventId);
    }

    private DynamodbEvent generateEventWithoutEventName() throws IOException {
        return generateRequestEvent(null,
                null,
                null,
                null,
                Collections.emptyList(),
                null);
    }

    private IndexDocument generateIndexDocument(String identifier,
                                                List<Contributor> contributors,
                                                String mainTitle,
                                                String type,
                                                String date) {
        List<IndexContributor> indexContributors = contributors.stream()
            .map(this::generateIndexContributor)
            .collect(Collectors.toList());

        return new IndexDocument.Builder()
            .withTitle(mainTitle)
            .withType(type)
            .withId(identifier)
            .withContributors(indexContributors)
            .withDate(date)
            .build();
    }

    private IndexContributor generateIndexContributor(Contributor contributor) {
        return new IndexContributor.Builder()
            .withId(contributor.getIdentity().getArpId())
            .withName(contributor.getIdentity().getName())
            .build();
    }

    private Contributor generateContributor(String identifier, String name, int sequence) {
        Identity identity = new Identity.Builder()
            .withArpId(identifier)
            .withId(URI.create(EXAMPLE_URI_BASE + identifier))
            .withName(name)
            .build();
        try {
            return new Contributor.Builder()
                .withIdentity(identity)
                .withSequence(sequence)
                .build();
        } catch (MalformedContributorException e) {
            throw new RuntimeException("The Contributor in generateContributor is malformed");
        }
    }

    private void updateDate(String date, JsonNode event) {
        if (nonNull(date)) {
            String[] splitDate = date.split(DATE_SEPARATOR);
            if (isYearOnly(splitDate)) {
                updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_YEAR_NAME, splitDate[YEAR_INDEX]);
            }
            if (isYearAndMonth(splitDate)) {
                updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_MONTH_NAME, splitDate[MONTH_INDEX]);
            }
            if (isYearMonthDay(splitDate)) {
                updateEventAtPointerWithNameAndValue(event, ENTITY_DESCRIPTION_PUBLICATION_DATE_JSON_POINTER,
                    EVENT_DAY_NAME, splitDate[DAY_INDEX]);
            }
        }
    }

    private boolean isYearMonthDay(String[] splitDate) {
        return splitDate.length == 3;
    }

    private boolean isYearAndMonth(String[] splitDate) {
        return splitDate.length >= 2;
    }

    private boolean isYearOnly(String[] splitDate) {
        return splitDate.length >= 1;
    }

    private void updateEventAtPointerWithNameAndValue(JsonNode event, String pointer, String name, Object value) {
        if (value instanceof String) {
            ((ObjectNode) event.at(pointer)).put(name, (String) value);
        } else {
            ((ObjectNode) event.at(pointer)).put(name, (Integer) value);
        }
    }

    private void updateEventAtPointerWithNameAndArrayValue(ObjectNode event,
                                                           String pointer,
                                                           String name,
                                                           ArrayNode value) {
        ((ObjectNode) event.at(pointer)).set(name, value);
    }

    private JsonNode extractRequestBodyFromEvent(DynamodbEvent requestEvent) {
        IndexDocument indexDocument = new DynamoDBEventTransformer()
                .parseStreamRecord(requestEvent.getRecords().get(0));
        return mapper.valueToTree(indexDocument);
    }

    private DynamodbEvent toDynamodbEvent(JsonNode event) {
        return mapper.convertValue(event, DynamodbEvent.class);
    }

    private DynamodbEvent loadEventFromResourceFile(String filename) throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(Paths.get(filename));
        return mapper.readValue(is, DynamodbEvent.class);
    }

    private DynamodbEvent generateRequestEvent(String eventName,
                                               String identifier,
                                               String type,
                                               String mainTitle,
                                               List<Contributor> contributors,
                                               String date) throws IOException {
        ObjectNode event = getEventTemplate();
        updateEventImageIdentifier(identifier, event);
        updateEventName(eventName, event);
        updateReferenceType(type, event);
        updateEntityDescriptionMainTitle(mainTitle, event);
        updateEntityDescriptionContributors(contributors, event);
        updateDate(date, event);
        return toDynamodbEvent(event);
    }

    private void updateEventImageIdentifier(String identifier, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, IMAGE_IDENTIFIER_JSON_POINTER, EVENT_JSON_STRING_NAME, identifier);
    }

    private ObjectNode getEventTemplate() throws IOException {
        return mapper.valueToTree(loadEventFromResourceFile(EVENT_TEMPLATE_JSON));
    }

    private void updateEntityDescriptionContributors(List<Contributor> contributors, ObjectNode event) {
        ArrayNode contributorsArrayNode = mapper.createArrayNode();
        if (nonNull(contributors)) {
            contributors.forEach(contributor -> updateContributor(contributorsArrayNode, contributor));
            updateEventAtPointerWithNameAndArrayValue(event, CONTRIBUTOR_POINTER, EVENT_JSON_LIST_NAME,
                contributorsArrayNode);
            ((ObjectNode) event.at(CONTRIBUTOR_POINTER)).set(EVENT_JSON_LIST_NAME, contributorsArrayNode);
        }
    }

    private void updateContributor(ArrayNode contributors, Contributor contributor) {
        ObjectNode activeTemplate = contributorTemplate.deepCopy();
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_SEQUENCE_POINTER,
            EVENT_JSON_STRING_NAME, contributor.getSequence());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_NAME_POINTER,
            EVENT_JSON_STRING_NAME, contributor.getIdentity().getName());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_ARPID_POINTER,
            EVENT_JSON_STRING_NAME, contributor.getIdentity().getArpId());
        updateEventAtPointerWithNameAndValue(activeTemplate, CONTRIBUTOR_ID_POINTER,
            EVENT_JSON_STRING_NAME, contributor.getIdentity().getId().toString());
        contributors.add(activeTemplate);
    }

    private void updateEntityDescriptionMainTitle(String mainTitle, ObjectNode event) {
        ((ObjectNode) event.at(ENTITY_DESCRIPTION_MAIN_TITLE_POINTER))
            .put(EVENT_JSON_STRING_NAME, mainTitle);
    }

    private void updateReferenceType(String type, ObjectNode event) {
        updateEventAtPointerWithNameAndValue(event, PUBLICATION_INSTANCE_TYPE_POINTER,
            EVENT_JSON_STRING_NAME, type);
    }

    private void updateEventName(String eventName, ObjectNode event) {
        ((ObjectNode) event.at(FIRST_RECORD_POINTER)).put(EVENT_NAME, eventName);
    }
}
