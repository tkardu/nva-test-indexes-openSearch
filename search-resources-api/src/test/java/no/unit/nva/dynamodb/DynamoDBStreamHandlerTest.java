package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Contributor;
import no.unit.nva.model.Identity;
import no.unit.nva.model.exceptions.MalformedContributorException;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.exception.InputException;
import nva.commons.utils.Environment;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static no.unit.nva.dynamodb.DynamoDBEventTransformer.MISSING_FIELD_LOGGER_WARNING_TEMPLATE;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.INSERT;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.MODIFY;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.REMOVE;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.SUCCESS_MESSAGE;
import static no.unit.nva.dynamodb.DynamoDBStreamHandler.UPSERT_EVENTS;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static nva.commons.utils.Environment.ENVIRONMENT_VARIABLE_NOT_SET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamoDBStreamHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String EXAMPLE_URI_BASE = "https://example.org/wanderlust/";
    public static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String UNKNOWN_EVENT = "UnknownEvent";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    public static final String EVENT_ID = "eventID";
    private static final String UNKNOWN_OPERATION_ERROR = "Not a known operation";
    public static final URI EXAMPLE_ID = URI.create("https://example.org/publication/irrelevant");
    public static final String EXAMPLE_TYPE = "JournalArticle";
    public static final String EXAMPLE_TITLE = "Some title";
    public static final String PLACEHOLDER_LOGS = "{}";
    public static final String PLACEHOLDER_STRINGS = "%s";
    public static final String TYPE = "type";
    public static final String TITLE = "title";
    public static final String EMPTY_STRING = "";
    public static final String EXPECTED_EVENT_ID = "12345";
    private static final String EXPECTED_MESSAGE = "expectedMessage";
    private static final String EXPECTED_LOG_MESSAGE_TEMPLATE =
            MISSING_FIELD_LOGGER_WARNING_TEMPLATE.replace(PLACEHOLDER_LOGS, PLACEHOLDER_STRINGS);
    public static final String WHITESPACE = "   ";
    private static final String SAMPLE_JSON_RESPONSE = "{}";

    private DynamoDBStreamHandler handler;
    private Context context;
    private Environment environment;
    private TestAppender testAppender;
    private RestHighLevelClient restClient;
    private SearchResponse searchResponse;

    /**
     * Set up test environment.
     *
     */
    @BeforeEach
    void init() {
        context = mock(Context.class);
        environment = setupMockEnvironment();
        restClient = mock(RestHighLevelClient.class);
        searchResponse = mock(SearchResponse.class);

        var elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment,
                restClient);
        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
        testAppender = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
    }

    @Test
    void constructorThrowsIllegalStateExceptionWhenEnvironmentIsNull() {
        Exception exception = assertThrows(IllegalStateException.class, DynamoDBStreamHandler::new);
        assertThat(exception.getMessage(), containsString(ENVIRONMENT_VARIABLE_NOT_SET));
    }

    @Test
    void handlerReturnsSuccessMessageWhenDocumentDoesNotExist() throws IOException {
        setUpDeleteResponseWithSuccess();

        String response = handler.handleRequest(generateValidRemoveEvent(), context);
        assertThat(response, containsString(SUCCESS_MESSAGE));
    }

    @Test
    void handleRequestThrowsExceptionWhenInputIsUnknownEventName() {

        Executable executable = () -> handler.handleRequest(generateEventWithEventName(UNKNOWN_EVENT), context);
        var exception = assertThrows(RuntimeException.class, executable);

        Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(InputException.class));
        assertThat(cause.getMessage(), containsString(UNKNOWN_OPERATION_ERROR));
    }

    @Test
    void handleRequestLogsErrorWhenInputIsUnknownEventName() {

        Executable executable = () -> handler.handleRequest(generateEventWithEventName(UNKNOWN_EVENT), context);
        assertThrows(RuntimeException.class, executable);

        assertThat(testAppender.getMessages(), containsString(UNKNOWN_EVENT));
        assertThat(testAppender.getMessages(), containsString(EXPECTED_EVENT_ID));
    }

    @Test
    void handleRequestThrowsExceptionAndLogsErrorWhenInputHasNoEventName() {
        Executable executable = () -> handler.handleRequest(generateEventWithoutEventName(), context);
        RuntimeException exception = assertThrows(RuntimeException.class, executable);

        Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(InputException.class));
        assertThat(cause.getMessage(), containsString(DynamoDBStreamHandler.EMPTY_EVENT_NAME_ERROR));

        assertThat(testAppender.getMessages(), containsString(DynamoDBStreamHandler.LOG_MESSAGE_MISSING_EVENT_NAME));
    }

    @ParameterizedTest
    @DisplayName("handler returns success message when event type is {0}")
    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    void handlerReturnsSuccessWhenEventNameIsValid(String eventName) throws IOException {
        setUpEventResponse(eventName);
        String request = handler.handleRequest(generateEventWithEventName(eventName), context);
        assertThat(request, equalTo(SUCCESS_MESSAGE));
    }

    @ParameterizedTest
    @DisplayName("handleRequestThrowsExceptionAndLogsErrorWhenInputIsBlankString: {0}")
    @ValueSource(strings = {EMPTY_STRING, WHITESPACE})
    void handleRequestThrowsExceptionAndLogsErrorWhenInputIsEmptyString(String empty) {
        Executable executable = () -> handler.handleRequest(generateEventWithEventName(empty), context);
        RuntimeException exception = assertThrows(RuntimeException.class, executable);

        Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(InputException.class));
        assertThat(cause.getMessage(), containsString(DynamoDBStreamHandler.EMPTY_EVENT_NAME_ERROR));

        assertThat(testAppender.getMessages(), containsString(DynamoDBStreamHandler.LOG_MESSAGE_MISSING_EVENT_NAME));
    }

    @ParameterizedTest
    @DisplayName("handleRequest throws RuntimeException when rest client called with {0} throws exception")
    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    void handleRequestThrowsRuntimeExceptionWhenRestClientThrowsIoException(String eventName)
            throws IOException {

        Exception expectedException = new IOException(EXPECTED_MESSAGE);
        setUpRestClientInError(eventName, expectedException);

        var elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);

        handler = new DynamoDBStreamHandler(elasticSearchRestClient);

        Executable executable = () -> handler.handleRequest(generateEventWithEventName(eventName), context);
        var exception = assertThrows(RuntimeException.class, executable);

        assertThat(exception.getMessage(), containsString(expectedException.getMessage()));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, Accepted")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithModifyEventValidRecord()
            throws IOException {
        DynamodbEvent requestEvent = getDynamoDbEventWithCompleteEntityDescriptionSingleContributor();
        String actual = handler.handleRequest(requestEvent, context);
        assertThat(actual, equalTo(SUCCESS_MESSAGE));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, single author")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithContributorsWhenInputIsModifyEvent()
            throws IOException {
        DynamoDbTestDataGenerator testData = generateTestDataWithSingleContributor();

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with complete record, multiple authors")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenInputIsModifyEvent()
            throws IOException {
        var requestEvent = generateTestData(generateContributors());
        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent.asDynamoDbEvent());
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
        var expected = requestEvent.asIndexDocument();

        assertThat(actual, equalTo(expected));
    }

    @ParameterizedTest
    @DisplayName("dynamoDBStreamHandler transforms strangely formatted dates: {0}-{1}-{2}")
    @CsvSource(value = {
            "2001,01,01",
            "2001,0,NULL",
            "2001,NULL,NULL",
            "NULL,NULL,NULL",
            "NULL,01,NULL",
            "NULL,01,01",
            "NULL,NULL,01"}, delimiter = ',', nullValues = "NULL")
    void dynamoDbStreamHandlerTransformsStrangelyFormattedDates(String year, String month, String day) throws
            IOException {
        IndexDate date = new IndexDate(year, month, day);

        var testData = generateTestData(date);
        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with empty Contributor list")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithEmptyContributorListWhenInputIsModifyEvent()
            throws IOException {

        var testData = generateTestData(Collections.emptyList());

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with no contributors or date")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithNoContributorsOrDateWhenInputIsModifyEvent()
            throws IOException {

        DynamoDbTestDataGenerator requestEvent = new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .build();

        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent.asDynamoDbEvent());
        IndexDocument expected = requestEvent.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
        assertThat(actual, samePropertyValuesAs(expected));
    }

    @Test
    @DisplayName("DynamoDBStreamHandler ignores Publications with no type and logs warning")
    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoType() throws IOException {
        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(DynamoDBEventTransformer.class);

        DynamodbEvent requestEvent = new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(EXAMPLE_ID)
                .withMainTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();

        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));

        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TYPE, EXAMPLE_ID);
        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    }

    @Test
    @DisplayName("DynamoDBStreamHandler ignores Publications with no title and logs warning")
    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoTitle() throws IOException {
        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(DynamoDBEventTransformer.class);

        DynamodbEvent requestEvent = new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .build()
                .asDynamoDbEvent();

        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));

        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TITLE, EXAMPLE_ID);
        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    }

    private DynamodbEvent getDynamoDbEventWithCompleteEntityDescriptionSingleContributor() throws IOException {
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";

        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(URI.create("https://example.org/publication/1006a"))
                .withType("Book")
                .withMainTitle("Moi buki")
                .withDate(new IndexDate("2020", "09", "08"))
                .withContributors(Collections.singletonList(
                        generateContributor(contributorIdentifier, contributorName, 1)))
                .build()
                .asDynamoDbEvent();
    }

    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
                .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
                .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        return environment;
    }

    private void setUpEventResponse(String eventName) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            when(searchResponse.toString()).thenReturn(SAMPLE_JSON_RESPONSE);
        } else {
            setUpDeleteResponseWithSuccess();
        }
    }

    private DynamodbEvent generateEventWithoutEventName() throws IOException {
        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();
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

    private void setUpRestClientInError(String eventName, Exception expectedException) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            when(restClient.update(any(), any())).thenThrow(expectedException);
        } else {
            when(restClient.delete(any(), any())).thenThrow(expectedException);
        }
    }

    private void setUpDeleteResponseWithSuccess() throws IOException {
        var deleteResponse = mock(DeleteResponse.class);
        when(restClient.delete(any(), any())).thenReturn(deleteResponse);
    }

    private JsonNode extractRequestBodyFromEvent(DynamodbEvent requestEvent) {
        IndexDocument indexDocument = new DynamoDBEventTransformer()
                .parseStreamRecord(requestEvent.getRecords().get(0));
        return mapper.valueToTree(indexDocument);
    }

    private DynamodbEvent generateValidRemoveEvent() throws IOException {
        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(REMOVE)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();
    }

    private DynamodbEvent generateEventWithEventName(String eventName) throws IOException {
        return new DynamoDbTestDataGenerator.Builder()
                .withEventName(eventName)
                .withEventId(EXPECTED_EVENT_ID)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();
    }

    private DynamoDbTestDataGenerator generateTestData(IndexDate date) throws IOException {
        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .withDate(date)
                .build();
    }

    private DynamoDbTestDataGenerator generateTestData(List<Contributor> contributors) throws IOException {
        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(EXAMPLE_ID)
                .withType(EXAMPLE_TYPE)
                .withMainTitle(EXAMPLE_TITLE)
                .withContributors(contributors)
                .build();
    }

    private DynamoDbTestDataGenerator generateTestDataWithSingleContributor() throws IOException {
        URI id = URI.create("https://example.org/publication/1006a");
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
                generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "Book";
        IndexDate date = new IndexDate("2020", "09", "08");

        return new DynamoDbTestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(id)
                .withType(type)
                .withMainTitle(mainTitle)
                .withContributors(contributors)
                .withDate(date)
                .build();
    }

    private List<Contributor> generateContributors() {
        String firstContributorIdentifier = "123";
        String firstContributorName = "Bólsön Kölàdỳ";
        String secondContributorIdentifier = "345";
        String secondContributorName = "Mèrdok Hüber";
        List<Contributor> contributors = new ArrayList<>();
        contributors.add(generateContributor(firstContributorIdentifier, firstContributorName, 1));
        contributors.add(generateContributor(secondContributorIdentifier, secondContributorName, 2));
        return contributors;
    }
}
