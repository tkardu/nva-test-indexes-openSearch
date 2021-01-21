package no.unit.nva.publication;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Reference;
import no.unit.nva.model.contexttypes.Book;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.model.contexttypes.PublicationContext;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.model.instancetypes.book.BookMonograph;
import no.unit.nva.model.instancetypes.journal.JournalArticle;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexContributor;
import no.unit.nva.search.IndexDate;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.IndexDocumentGenerator;
import no.unit.nva.search.IndexPublisher;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static no.unit.nva.publication.DynamoDBStreamHandler.INSERT;
import static no.unit.nva.publication.DynamoDBStreamHandler.MODIFY;
import static no.unit.nva.publication.DynamoDBStreamHandler.REMOVE;
import static no.unit.nva.publication.DynamoDBStreamHandler.SUCCESS_MESSAGE;
import static no.unit.nva.publication.DynamoDBStreamHandler.UPSERT_EVENTS;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static no.unit.nva.search.IndexDocumentGenerator.ABSTRACT;
import static no.unit.nva.search.IndexDocumentGenerator.DESCRIPTION;
import static no.unit.nva.search.IndexDocumentGenerator.MISSING_FIELD_LOGGER_WARNING_TEMPLATE;
import static no.unit.nva.search.IndexDocumentGenerator.PUBLISHED;
import static no.unit.nva.search.IndexDocumentGenerator.STATUS;
import static nva.commons.utils.Environment.ENVIRONMENT_VARIABLE_NOT_SET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDBStreamHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String EXAMPLE_ARP_URI_BASE = "https://example.org/arp/";
    public static final String UNKNOWN_EVENT = "UnknownEvent";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    public static final String EVENT_ID = "eventID";
    private static final String UNKNOWN_OPERATION_ERROR = "Not a known operation";
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
    public static final String DRAFT = "DRAFT";
    public static final String OWNER = "jd@not.here";
    private static final URI SAMPLE_DOI = URI.create("https://doi.org/10.1103/physrevd.100.085005");

    private static final URI SAMPLE_PUBLISHER_ID =
            URI.create("https://api.dev.nva.aws.unit.no/customer/f54c8aa9-073a-46a1-8f7c-dde66c853934");
    private static final String SAMPLE_PUBLISHER_NAME = "Organization";
    private static final IndexPublisher SAMPLE_PUBLISHER = new IndexPublisher.Builder()
            .withId(SAMPLE_PUBLISHER_ID).withName(SAMPLE_PUBLISHER_NAME).build();
    public static final Instant SAMPLE_MODIFIED_DATE = Instant.now();
    public static final Instant SAMPLE_PUBLISHED_DATE = Instant.now();
    public static final int NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE = 2;
    public static final Map<String, String> SAMPLE_ALTERNATIVETITLES  = Map.of("a", "b","c", "d");


    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String BOOK_MONOGRAPH_TYPE = "BookMonograph";
    public static final String SAMPLE_TITLE2 = "Moi buki";
    private DynamoDBStreamHandler handler;
    private Context context;
    private Environment environment;
    private TestAppender testAppender;
    private RestHighLevelClient restClient;
    private SearchResponse searchResponse;

    private static final Reference SAMPLE_JOURNAL_REFERENCE = createJournalReference();
    private static final Reference SAMPLE_BOOK_REFERENCE = createBookReference();

    private static Reference createBookReference() {
        PublicationInstance publicationInstance = new BookMonograph.Builder().build();
        PublicationContext publicationContext = null;
        try {
            publicationContext = new Book.Builder().build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Reference.Builder()
                .withPublicationInstance(publicationInstance)
                .withPublishingContext(publicationContext)
                .withDoi(SAMPLE_DOI)
                .build();
    }

    private static Reference createJournalReference() {
        PublicationInstance publicationInstance = new JournalArticle.Builder().build();
        PublicationContext publicationContext = null;
        try {
            publicationContext = new Journal.Builder().build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Reference.Builder()
                .withPublicationInstance(publicationInstance)
                .withPublishingContext(publicationContext)
                .withDoi(SAMPLE_DOI)
                .build();
    }

    /**
     * Set up test environment.
     */
    @BeforeEach
    void init() {
        context = mock(Context.class);
        environment = setupMockEnvironment();
        restClient = mock(RestHighLevelClient.class);
        searchResponse = mock(SearchResponse.class);

        var elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);
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

        verifyRestHighLevelClientInvokedOnRemove();
        assertThat(response, containsString(SUCCESS_MESSAGE));
    }

    @Test
    void handlerReturnsSuccessMessageWhenDocumentToRemoveIsNotPublished() throws IOException {
        setUpDeleteResponseWithSuccess();

        DynamodbEvent event = generateMinimalValidRemoveEvent();
        String response = handler.handleRequest(event, context);

        verifyRestHighLevelClientInvokedOnRemove();
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
    void handleRequestLogsErrorWhenInputEventNameIsNull() {
        Executable executable = () -> {
            DynamodbEvent dynamodbEvent = generateEventWithEventName(null);
            handler.handleRequest(dynamodbEvent, context);
        };
        assertThrows(RuntimeException.class, executable);
        assertThat(testAppender.getMessages(), containsString(DynamoDBStreamHandler.LOG_MESSAGE_MISSING_EVENT_NAME));
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

        verifyRestHighLevelClientInvocation(eventName);
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
        TestDataGenerator testData = generateTestDataWithSingleContributor();

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
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

    @Test
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenContributorIdIsIRI()
            throws IOException {
        var dynamoDbStreamRecord =
                new TestDataGenerator.Builder().build().getSampleDynamoDBStreamRecord();
        IndexDocument document = IndexDocumentGenerator.fromJsonNode(dynamoDbStreamRecord);
        assertNotNull(document);

        List<IndexContributor> indexContributors = document.getContributors();
        var ids = indexContributors.stream()
                .map(IndexContributor::getId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        assertThat("Contributors has id", ids.size() == NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE);
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

        assertThat(actual, equalTo(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with empty Contributor list")
    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithEmptyContributorListWhenInputIsModifyEvent()
            throws IOException {

        var testData = generateTestData(Collections.emptyList());

        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());

        IndexDocument expected = testData.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    @DisplayName("Test dynamoDBStreamHandler with no contributors or date")
    public void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithNoContributorsOrDateWhenInputIsModifyEvent()
            throws IOException {

        TestDataGenerator requestEvent = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(generateValidId())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .withDoi(SAMPLE_DOI)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(Instant.now())
                .withPublishedDate(Instant.now())
                .withReference(SAMPLE_JOURNAL_REFERENCE)
                .build();

        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent.asDynamoDbEvent());
        IndexDocument expected = requestEvent.asIndexDocument();
        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    @DisplayName("DynamoDBStreamHandler ignores Publications with no type and logs warning")
    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoType() throws IOException {
        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(IndexDocumentGenerator.class);

        UUID id = generateValidId();
        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(id)
                .withTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();

        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));

        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TYPE, id);
        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    }

    @Test
    @DisplayName("DynamoDBStreamHandler ignores Publications with no title and logs warning")
    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoTitle() throws IOException {
        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(IndexDocumentGenerator.class);
        UUID id = generateValidId();
        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(id)
                .withType(EXAMPLE_TYPE)
                .build()
                .asDynamoDbEvent();

        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));

        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TITLE, id);
        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    }

    @Test
    void dynamoDBStreamHandlerIgnoresPublicationsWhenStatusIsNotPublished() throws IOException {
        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
                .withStatus(DRAFT)
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(UUID.randomUUID())
                .withType(EXAMPLE_TYPE)
                .build()
                .asDynamoDbEvent();
        handler.handleRequest(requestEvent, context);

        restClientIsNotInvoked();
    }

    @Test
    void dynamoDBStreamHandlerLogsMissingStatus() throws IOException {
        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
        UUID id = UUID.randomUUID();
        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(id)
                .withType(EXAMPLE_TYPE)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .build()
                .asDynamoDbEvent();
        handler.handleRequest(requestEvent, context);
        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, STATUS, id);
        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    }

    @Test
    void dynamoDBStreamHandlerIgnoresPublicationsThatHaveNoStatus() throws IOException {
        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withId(UUID.randomUUID())
                .withType(EXAMPLE_TYPE)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .build()
                .asDynamoDbEvent();
        handler.handleRequest(requestEvent, context);
        restClientIsNotInvoked();
    }

    private void restClientIsNotInvoked() throws IOException {
        verify(restClient, (never())).update(any(), any());
    }

    private DynamodbEvent getDynamoDbEventWithCompleteEntityDescriptionSingleContributor() throws IOException {
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";

        return new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(MODIFY)
                .withStatus(PUBLISHED)
                .withId(generateValidId())
                .withType(BOOK_MONOGRAPH_TYPE)
                .withTitle(SAMPLE_TITLE2)
                .withDate(new IndexDate("2020", "09", "08"))
                .withContributors(Collections.singletonList(
                        generateContributor(contributorIdentifier, contributorName, 1)))
                .withOwner(OWNER)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
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
        return new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withId(UUID.randomUUID())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .build()
                .asDynamoDbEvent();
    }

    private Contributor generateContributor(String identifier, String name, int sequence) {
        return new Contributor(sequence, name, identifier, URI.create(EXAMPLE_ARP_URI_BASE + identifier));
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
        IndexDocument indexDocument = IndexDocumentGenerator
                .fromStreamRecord(requestEvent.getRecords().get(0))
                .toIndexDocument();
        return mapper.valueToTree(indexDocument);
    }

    private DynamodbEvent generateValidRemoveEvent() throws IOException {
        return new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(REMOVE)
                .withId(UUID.randomUUID())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .build()
                .asDynamoDbEvent();
    }

    private DynamodbEvent generateMinimalValidRemoveEvent() throws IOException {
        DynamodbEvent event = new TestDataGenerator.Builder()
                .withEventId(EVENT_ID)
                .withEventName(REMOVE)
                .withId(UUID.randomUUID())
                .build()
                .asDynamoDbEvent();

        event.getRecords().get(0).getDynamodb().clearNewImageEntries();
        return event;
    }

    private DynamodbEvent generateEventWithEventName(String eventName) throws IOException {
        return new TestDataGenerator.Builder()
                .withReference(SAMPLE_JOURNAL_REFERENCE)
                .withEventName(eventName)
                .withStatus(PUBLISHED)
                .withEventId(EXPECTED_EVENT_ID)
                .withId(UUID.randomUUID())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withOwner(OWNER)
                .withDoi(SAMPLE_DOI)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .build()
                .asDynamoDbEvent();
    }

    private TestDataGenerator generateTestData(IndexDate date) throws IOException {
        return new TestDataGenerator.Builder()
                .withReference(SAMPLE_JOURNAL_REFERENCE)
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(generateValidId())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .withDate(date)
                .withOwner(OWNER)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withDoi(SAMPLE_DOI)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .withPublishedDate(SAMPLE_PUBLISHED_DATE)
                .withAlternativeTitles(SAMPLE_ALTERNATIVETITLES)
                .build();
    }

    private TestDataGenerator generateTestData(List<Contributor> contributors) throws IOException {
        return new TestDataGenerator.Builder()
                .withReference(SAMPLE_JOURNAL_REFERENCE)
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(generateValidId())
                .withType(EXAMPLE_TYPE)
                .withTitle(EXAMPLE_TITLE)
                .withContributors(contributors)
                .withOwner(OWNER)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withDoi(SAMPLE_DOI)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .withPublishedDate(SAMPLE_PUBLISHED_DATE)
                .withAlternativeTitles(SAMPLE_ALTERNATIVETITLES)
                .build();
    }

    private TestDataGenerator generateTestDataWithSingleContributor() throws IOException {
        UUID id = generateValidId();
        String contributorIdentifier = "123";
        String contributorName = "Bólsön Kölàdỳ";
        List<Contributor> contributors = Collections.singletonList(
                generateContributor(contributorIdentifier, contributorName, 1));
        String mainTitle = "Moi buki";
        String type = "BookMonograph";
        IndexDate date = new IndexDate("2020", "09", "08");

        return new TestDataGenerator.Builder()
                .withReference(createBookReference())
                .withEventId(EVENT_ID)
                .withStatus(PUBLISHED)
                .withEventName(MODIFY)
                .withId(id)
                .withType(type)
                .withTitle(mainTitle)
                .withContributors(contributors)
                .withDate(date)
                .withOwner(OWNER)
                .withDescription(DESCRIPTION)
                .withAbstract(ABSTRACT)
                .withDoi(SAMPLE_DOI)
                .withPublisher(SAMPLE_PUBLISHER)
                .withModifiedDate(SAMPLE_MODIFIED_DATE)
                .withPublishedDate(SAMPLE_PUBLISHED_DATE)
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

    private void verifyRestHighLevelClientInvocation(String eventName) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            verify(restClient, (atMostOnce())).update(any(), any());
            verify(restClient, (times(1))).update(any(), any());
        } else {
            verify(restClient, (atMostOnce())).update(any(), any());
            verify(restClient, (times(1))).delete(any(), any());
        }
    }

    private void verifyRestHighLevelClientInvokedOnRemove() throws IOException {
        verifyRestHighLevelClientInvocation(REMOVE);
    }

    private UUID generateValidId() {
        return UUID.randomUUID();
    }
}
