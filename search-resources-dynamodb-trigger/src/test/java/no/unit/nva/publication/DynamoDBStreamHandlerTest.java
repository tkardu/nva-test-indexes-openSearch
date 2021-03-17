package no.unit.nva.publication;

import static no.unit.nva.model.PublicationStatus.DRAFT;
import static no.unit.nva.model.PublicationStatus.PUBLISHED;
import static no.unit.nva.publication.DynamoDBStreamHandler.INSERT;
import static no.unit.nva.publication.DynamoDBStreamHandler.MODIFY;
import static no.unit.nva.publication.DynamoDBStreamHandler.REMOVE;
import static no.unit.nva.publication.DynamoDBStreamHandler.SUCCESS_MESSAGE;
import static no.unit.nva.publication.DynamoDBStreamHandler.UPSERT_EVENTS;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static no.unit.nva.search.IndexDocumentGenerator.MISSING_FIELD_LOGGER_WARNING_TEMPLATE;
import static nva.commons.core.Environment.ENVIRONMENT_VARIABLE_NOT_SET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.UUID;
import no.unit.nva.model.Reference;
import no.unit.nva.model.contexttypes.Book;
import no.unit.nva.model.contexttypes.Journal;
import no.unit.nva.model.contexttypes.PublicationContext;
import no.unit.nva.model.exceptions.InvalidIssnException;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.model.instancetypes.book.BookMonograph;
import no.unit.nva.model.instancetypes.journal.JournalArticle;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import nva.commons.core.Environment;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class DynamoDBStreamHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String EXAMPLE_ARP_URI_BASE = "https://example.org/arp/";
    public static final String PLACEHOLDER_LOGS = "{}";
    public static final String PLACEHOLDER_STRINGS = "%s";

    public static final String UNKNOWN_EVENT = "unknownEvent";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    private static final String EXPECTED_LOG_MESSAGE_TEMPLATE =
        MISSING_FIELD_LOGGER_WARNING_TEMPLATE.replace(PLACEHOLDER_LOGS, PLACEHOLDER_STRINGS);
    private static final String SAMPLE_JSON_RESPONSE = "{}";
    private static final URI SAMPLE_DOI = URI.create("https://doi.org/10.1103/physrevd.100.085005");
    private static final Reference SAMPLE_JOURNAL_REFERENCE = createJournalReference();
    private static final Reference SAMPLE_BOOK_REFERENCE = createBookReference();
    private DynamoDBStreamHandler handler;
    private Context context;
    private Environment environment;
    private TestAppender testAppender;
    private RestHighLevelClient restClient;
    private ByteArrayOutputStream output;
    private TestDataGenerator dataGenerator;
    private ElasticSearchHighLevelRestClient elasticSearchRestClient;

    /**
     * Set up test environment.
     */
    @BeforeEach
    void init() throws IOException {
        context = mock(Context.class);
        environment = setupMockEnvironment();
        restClient = mockElasticSearch();
        output = new ByteArrayOutputStream();
        dataGenerator = new TestDataGenerator();
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);
        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
        testAppender = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
    }

    @Test
    void constructorThrowsIllegalStateExceptionWhenEnvironmentIsNull() {
        Exception exception = assertThrows(IllegalStateException.class, DynamoDBStreamHandler::new);
        assertThat(exception.getMessage(), containsString(ENVIRONMENT_VARIABLE_NOT_SET));
    }

    @Test
    void handlerReturnsSuccessMessageWhenDeletingDocument() throws IOException, InvalidIssnException {
        handler.handleRequest(dataGenerator.deletePublishedResourceEvent(), output, context);
        String response = output.toString();
        verifyRestHighLevelClientInvokedOnRemove();
        assertThat(response, containsString(SUCCESS_MESSAGE));
    }

    @Test
    void handlerDoesNotSendRequestToElasticSearchWhenResourceInNotPublished() throws IOException, InvalidIssnException {
        InputStream event = dataGenerator.createResourceEvent(MODIFY, DRAFT, DRAFT);
        handler.handleRequest(event, output, context);
        String response = output.toString();
        verifyRestClientIsNotInvoked();
        assertThat(response, containsString(SUCCESS_MESSAGE));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {UNKNOWN_EVENT})
    void handleRequestThrowsExceptionWhenInputIsUnknownEventName(String eventType)
        throws InvalidIssnException, MalformedURLException, JsonProcessingException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        InputStream input = dataGenerator.createResourceEvent(eventType, PUBLISHED, PUBLISHED);
        Executable action = () -> handler.handleRequest(input, output, context);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);
        assertThat(exception.getMessage(), containsString(DynamoDBStreamHandler.UNKNOWN_OPERATION_ERROR));
        String eventTypeStringRepresentation = String.format("%s", eventType);

        assertThat(exception.getMessage(), containsString(eventTypeStringRepresentation));
        assertThat(appender.getMessages(),containsString(exception.getMessage()));
    }

    @ParameterizedTest(name="handler invokes elastic search client when event is valid and is: {0}")
    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    void handlerInvokesElasticSearchClientWhenEventTypeisValid(String eventType) throws IOException, InvalidIssnException {
        InputStream input = dataGenerator.createResourceEvent(eventType,PUBLISHED,PUBLISHED);

        handler.handleRequest(input,output, context);
        String response = output.toString();
        verifyRestHighLevelClientInvocation(eventType);
        assertThat(response, containsString(SUCCESS_MESSAGE));
    }





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

    private RestHighLevelClient mockElasticSearch() throws IOException {
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        DeleteResponse fakeDeleteResponse = mockDeleteResponse();
        when(client.delete(any(DeleteRequest.class), any(RequestOptions.class)))
            .thenReturn(fakeDeleteResponse);
        return client;
    }

    private DeleteResponse mockDeleteResponse() {
        DeleteResponse fakeDeleteResponse = mock(DeleteResponse.class);
        when(fakeDeleteResponse.getResult()).thenReturn(Result.NOT_FOUND);
        return fakeDeleteResponse;
    }


    //
    //    @ParameterizedTest
    //    @DisplayName("handleRequestThrowsExceptionAndLogsErrorWhenInputIsBlankString: {0}")
    //    @ValueSource(strings = {EMPTY_STRING, WHITESPACE})
    //    void handleRequestThrowsExceptionAndLogsErrorWhenInputIsEmptyString(String empty) {
    //        Executable executable = () -> handler.handleRequest(generateEventWithEventName(empty), context);
    //        RuntimeException exception = assertThrows(RuntimeException.class, executable);
    //
    //        Throwable cause = exception.getCause();
    //
    //        assertThat(cause, instanceOf(InputException.class));
    //        assertThat(cause.getMessage(), containsString(DynamoDBStreamHandler.EMPTY_EVENT_NAME_ERROR));
    //
    //        assertThat(testAppender.getMessages(), containsString(DynamoDBStreamHandler
    //        .LOG_MESSAGE_MISSING_EVENT_NAME));
    //    }
    //
    //    @ParameterizedTest
    //    @DisplayName("handleRequest throws RuntimeException when rest client called with {0} throws exception")
    //    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    //    void handleRequestThrowsRuntimeExceptionWhenRestClientThrowsIoException(String eventName)
    //            throws IOException {
    //
    //        Exception expectedException = new IOException(EXPECTED_MESSAGE);
    //        setUpRestClientInError(eventName, expectedException);
    //
    //        var elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);
    //
    //        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
    //
    //        Executable executable = () -> handler.handleRequest(generateEventWithEventName(eventName), context);
    //        var exception = assertThrows(RuntimeException.class, executable);
    //
    //        assertThat(exception.getMessage(), containsString(expectedException.getMessage()));
    //    }
    //
    //    @Test
    //    @DisplayName("Test dynamoDBStreamHandler with complete record, Accepted")
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithModifyEventValidRecord()
    //            throws IOException {
    //        DynamodbEvent requestEvent = getDynamoDbEventWithCompleteEntityDescriptionSingleContributor();
    //        String actual = handler.handleRequest(requestEvent, context);
    //        assertThat(actual, equalTo(SUCCESS_MESSAGE));
    //    }
    //
    //    @Test
    //    @DisplayName("Test dynamoDBStreamHandler with complete record, single author")
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithContributorsWhenInputIsModifyEvent()
    //            throws IOException {
    //        TestDataGenerator testData = generateTestDataWithSingleContributor();
    //
    //        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());
    //
    //        IndexDocument expected = testData.asIndexDocument();
    //        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
    //
    //        assertThat(actual, equalTo(expected));
    //    }
    //
    //    @Test
    //    @DisplayName("Test dynamoDBStreamHandler with complete record, multiple authors")
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenInputIsModifyEvent()
    //            throws IOException {
    //        var requestEvent = generateTestData(generateContributors());
    //        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent.asDynamoDbEvent());
    //        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
    //        var expected = requestEvent.asIndexDocument();
    //
    //        assertThat(actual, equalTo(expected));
    //    }
    //
    //    @Test
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithMultipleContributorsWhenContributorIdIsIRI()
    //            throws IOException {
    //        var dynamoDbStreamRecord =
    //                new TestDataGenerator.Builder().build().getSampleDynamoDBStreamRecord();
    //        IndexDocument document = IndexDocumentGenerator.fromJsonNode(dynamoDbStreamRecord);
    //        assertNotNull(document);
    //
    //        List<IndexContributor> indexContributors = document.getContributors();
    //        var ids = indexContributors.stream()
    //                .map(IndexContributor::getId)
    //                .filter(Objects::nonNull)
    //                .collect(Collectors.toSet());
    //        assertThat("Contributors has id", ids.size() == NUMBER_OF_CONTRIBUTOR_IRIS_IN_SAMPLE);
    //    }
    //
    //
    //
    //    @ParameterizedTest
    //    @DisplayName("dynamoDBStreamHandler transforms strangely formatted dates: {0}-{1}-{2}")
    //    @CsvSource(value = {
    //            "2001,01,01",
    //            "2001,0,NULL",
    //            "2001,NULL,NULL",
    //            "NULL,NULL,NULL",
    //            "NULL,01,NULL",
    //            "NULL,01,01",
    //            "NULL,NULL,01"}, delimiter = ',', nullValues = "NULL")
    //    void dynamoDbStreamHandlerTransformsStrangelyFormattedDates(String year, String month, String day) throws
    //            IOException {
    //        IndexDate date = new IndexDate(year, month, day);
    //
    //        var testData = generateTestData(date);
    //        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());
    //
    //        IndexDocument expected = testData.asIndexDocument();
    //        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
    //
    //        assertThat(actual, equalTo(expected));
    //    }
    //
    //    @Test
    //    @DisplayName("Test dynamoDBStreamHandler with empty Contributor list")
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithEmptyContributorListWhenInputIsModifyEvent()
    //            throws IOException {
    //
    //        var testData = generateTestData(Collections.emptyList());
    //
    //        JsonNode requestBody = extractRequestBodyFromEvent(testData.asDynamoDbEvent());
    //
    //        IndexDocument expected = testData.asIndexDocument();
    //        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
    //
    //        assertThat(actual, equalTo(expected));
    //    }
    //
    //    @Test
    //    @DisplayName("Test dynamoDBStreamHandler with no contributors or date")
    //    void dynamoDBStreamHandlerCreatesHttpRequestWithIndexDocumentWithNoContributorsOrDateWhenInputIsModifyEvent()
    //            throws IOException {
    //
    //        TestDataGenerator requestEvent = new TestDataGenerator.Builder()
    //                .withEventId(EVENT_ID)
    //                .withStatus(PUBLISHED)
    //                .withEventName(MODIFY)
    //                .withId(generateValidId())
    //                .withType(EXAMPLE_TYPE)
    //                .withTitle(EXAMPLE_TITLE)
    //                .withDoi(SAMPLE_DOI)
    //                .withPublisher(SAMPLE_PUBLISHER)
    //                .withModifiedDate(Instant.now())
    //                .withPublishedDate(Instant.now())
    //                .withReference(SAMPLE_JOURNAL_REFERENCE)
    //                .build();
    //
    //        JsonNode requestBody = extractRequestBodyFromEvent(requestEvent.asDynamoDbEvent());
    //        IndexDocument expected = requestEvent.asIndexDocument();
    //        IndexDocument actual = mapper.convertValue(requestBody, IndexDocument.class);
    //
    //        assertThat(actual, equalTo(expected));
    //    }
    //
    //    @Test
    //    @DisplayName("DynamoDBStreamHandler ignores Publications with no type and logs warning")
    //    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoType() throws IOException {
    //        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(IndexDocumentGenerator.class);
    //
    //        UUID id = generateValidId();
    //        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
    //                .withEventId(EVENT_ID)
    //                .withStatus(PUBLISHED)
    //                .withEventName(MODIFY)
    //                .withId(id)
    //                .withTitle(EXAMPLE_TITLE)
    //                .build()
    //                .asDynamoDbEvent();
    //
    //        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));
    //
    //        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TYPE, id);
    //        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    //    }
    //
    //    @Test
    //    @DisplayName("DynamoDBStreamHandler ignores Publications with no title and logs warning")
    //    void dynamoDBStreamHandlerIgnoresPublicationsWhenPublicationHasNoTitle() throws IOException {
    //        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(IndexDocumentGenerator.class);
    //        UUID id = generateValidId();
    //        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
    //                .withEventId(EVENT_ID)
    //                .withStatus(PUBLISHED)
    //                .withEventName(MODIFY)
    //                .withId(id)
    //                .withType(EXAMPLE_TYPE)
    //                .build()
    //                .asDynamoDbEvent();
    //
    //        assertThat(handler.handleRequest(requestEvent, context), equalTo(SUCCESS_MESSAGE));
    //
    //        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, TITLE, id);
    //        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    //    }
    //
    //    @Test
    //    void dynamoDBStreamHandlerIgnoresPublicationsWhenStatusIsNotPublished() throws IOException {
    //        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
    //                .withStatus(DRAFT)
    //                .withEventId(EVENT_ID)
    //                .withEventName(MODIFY)
    //                .withId(UUID.randomUUID())
    //                .withType(EXAMPLE_TYPE)
    //                .build()
    //                .asDynamoDbEvent();
    //        handler.handleRequest(requestEvent, context);
    //
    //        restClientIsNotInvoked();
    //    }
    //
    //    @Test
    //    void dynamoDBStreamHandlerLogsMissingStatus() throws IOException {
    //        TestAppender testAppenderEventTransformer = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
    //        UUID id = UUID.randomUUID();
    //        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
    //                .withEventId(EVENT_ID)
    //                .withEventName(MODIFY)
    //                .withId(id)
    //                .withType(EXAMPLE_TYPE)
    //                .withModifiedDate(SAMPLE_MODIFIED_DATE)
    //                .build()
    //                .asDynamoDbEvent();
    //        handler.handleRequest(requestEvent, context);
    //        String expectedLogMessage = String.format(EXPECTED_LOG_MESSAGE_TEMPLATE, STATUS, id);
    //        assertThat(testAppenderEventTransformer.getMessages(), containsString(expectedLogMessage));
    //    }
    //
    //    @Test
    //    void dynamoDBStreamHandlerIgnoresPublicationsThatHaveNoStatus() throws IOException {
    //        DynamodbEvent requestEvent = new TestDataGenerator.Builder()
    //                .withEventId(EVENT_ID)
    //                .withEventName(MODIFY)
    //                .withId(UUID.randomUUID())
    //                .withType(EXAMPLE_TYPE)
    //                .withModifiedDate(SAMPLE_MODIFIED_DATE)
    //                .build()
    //                .asDynamoDbEvent();
    //        handler.handleRequest(requestEvent, context);
    //        restClientIsNotInvoked();
    //    }

    private void verifyRestClientIsNotInvoked() throws IOException {
        verify(restClient, (never())).update(any(), any());
        verify(restClient, (never())).delete(any(), any());
        verify(restClient, (never())).index(any(), any());
    }

    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        return environment;
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

    private void verifyRestHighLevelClientInvocation(String eventName) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            verify(restClient, (atMostOnce())).update(any(), any());
            verify(restClient, (times(1))).update(any(), any());
        } else {
            verify(restClient, (times(1))).delete(any(), any());
        }
    }

    private void verifyRestHighLevelClientInvokedOnRemove() throws IOException {
        verifyRestHighLevelClientInvocation(REMOVE);
    }


}
