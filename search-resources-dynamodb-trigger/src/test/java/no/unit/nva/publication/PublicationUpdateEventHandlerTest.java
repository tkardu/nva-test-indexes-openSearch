package no.unit.nva.publication;

import static java.util.Objects.nonNull;
import static no.unit.nva.model.PublicationStatus.DRAFT;
import static no.unit.nva.model.PublicationStatus.PUBLISHED;
import static no.unit.nva.publication.IndexAction.DELETE;
import static no.unit.nva.publication.IndexAction.INDEX;
import static no.unit.nva.publication.IndexAction.NO_ACTION;
import static no.unit.nva.publication.PublicationUpdateEventHandler.INSERT;
import static no.unit.nva.publication.PublicationUpdateEventHandler.INVALID_EVENT_ERROR;
import static no.unit.nva.publication.PublicationUpdateEventHandler.MODIFY;
import static no.unit.nva.publication.PublicationUpdateEventHandler.NO_TITLE_WARNING;
import static no.unit.nva.publication.PublicationUpdateEventHandler.NO_TYPE_WARNING;
import static no.unit.nva.publication.PublicationUpdateEventHandler.REMOVE;
import static no.unit.nva.publication.PublicationUpdateEventHandler.REMOVING_RESOURCE_WARNING;
import static no.unit.nva.publication.PublicationUpdateEventHandler.RESOURCE_IS_NOT_PUBLISHED_WARNING;
import static no.unit.nva.publication.PublicationUpdateEventHandler.UPSERT_EVENTS;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static nva.commons.core.Environment.ENVIRONMENT_VARIABLE_NOT_SET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
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
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.Arrays;
import no.unit.nva.model.PublicationStatus;
import no.unit.nva.model.exceptions.InvalidIssnException;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.IndexDocument;
import no.unit.nva.search.RestHighLevelClientWrapper;
import no.unit.nva.search.SearchResourcesResponse;
import nva.commons.apigateway.exceptions.ApiGatewayException;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;
import nva.commons.core.SingletonCollector;
import nva.commons.core.StringUtils;
import nva.commons.core.attempt.Try;
import nva.commons.core.ioutils.IoUtils;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class PublicationUpdateEventHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String UNKNOWN_EVENT = "unknownEvent";
    public static final String ELASTIC_SEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.11.2";
    public static final String HTTP_SCHEME = "http";
    public static final int SINGLE_RESULT = 1;
    public static final String SPACE = " ";
    public static final int TIME_FOR_DOCUMENT_TO_BECOME_AVAILABLE = 1_000;
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";

    private static final int FIRST_RESULT = 0;
    private static final String RUNTIME_EXCEPTION_MESSAGE = "RuntimeExceptionMessage";

    private PublicationUpdateEventHandler handler;
    private Context context;
    private Environment environment;
    private TestAppender testAppender;
    private RestHighLevelClientWrapper restClient;
    private ByteArrayOutputStream output;
    private TestDataGenerator dataGenerator;
    private ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private ElasticsearchContainer container;

    public RestHighLevelClientWrapper clientToLocalInstance() throws IOException {
        container = new ElasticsearchContainer(ELASTIC_SEARCH_IMAGE);
        container.start();

        HttpHost httpHost = new HttpHost(container.getHost(), container.getFirstMappedPort(), HTTP_SCHEME);

        RestClientBuilder builder = RestClient.builder(httpHost);
        RestHighLevelClientWrapper client = new RestHighLevelClientWrapper(new RestHighLevelClient(builder));
        createIndex(client);
        return client;
    }

    @BeforeEach
    public void init() throws IOException {
        context = mock(Context.class);
        environment = setupMockEnvironment();
        restClient = mockElasticSearch();
        output = new ByteArrayOutputStream();
        dataGenerator = new TestDataGenerator();
        elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);
        handler = new PublicationUpdateEventHandler(elasticSearchRestClient);
        testAppender = LogUtils.getTestingAppenderForRootLogger();
    }

    @AfterEach
    public void tearDownElasticSearch() {
        if (nonNull(container)) {
            container.close();
        }
        container = null;
    }

    @Test
    @Tag("ExcludedFromBuildIntegrationTest")
    public void handlerIndexesDocumentInEsWhenEventIsValidUpdate()
        throws InvalidIssnException, IOException, ApiGatewayException {
        elasticSearchRestClient = createHighLevelClientConnectedToLocalhost();

        InputStream inputStream = dataGenerator.createResourceEvent(MODIFY, DRAFT, PUBLISHED);

        handler.handleRequest(inputStream, output, context);
        waitForDocumentToBecomeAvailable();
        String publicationTitle = dataGenerator.getNewPublication().getEntityDescription().getMainTitle();

        String term = Arrays.stream(publicationTitle.split(SPACE)).findAny().orElseThrow();
        SearchResourcesResponse result = searchElasticSearch(term);

        IndexDocument indexedDocument = getSingleHit(result);
        IndexDocument expectedDocument = IndexDocument.fromPublication(dataGenerator.getNewPublication());

        assertThat(indexedDocument, is(equalTo(expectedDocument)));
    }

    @Test
    public void publicationUpdateEventHandlerIgnoresEntriesWithNoInstance()
        throws IOException, InvalidIssnException {

        InputStream inputStream = dataGenerator.createResourceWithNoInstance();
        handler.handleRequest(inputStream, output, context);
        verifyRestClientIsNotInvoked();

        assertThat(output.toString(), containsString(NO_ACTION.toString()));
        assertThat(testAppender.getMessages(), containsString(NO_TYPE_WARNING));
    }

    @Test
    public void publicationUpdateEventHandlerIgnoresEntriesWithNoTitle()
        throws IOException, InvalidIssnException {

        InputStream inputStream = dataGenerator.createResourceWithNoTitle();
        handler.handleRequest(inputStream, output, context);
        verifyRestClientIsNotInvoked();

        assertThat(output.toString(), containsString(NO_ACTION.toString()));
        assertThat(testAppender.getMessages(), containsString(NO_TITLE_WARNING));
    }

    @Test
    void constructorThrowsIllegalStateExceptionWhenEnvironmentIsNull() {
        Exception exception = assertThrows(IllegalStateException.class, PublicationUpdateEventHandler::new);
        assertThat(exception.getMessage(), containsString(ENVIRONMENT_VARIABLE_NOT_SET));
    }

    @Test
    void handlerReturnsSuccessMessageWhenDeletingDocument() throws IOException, InvalidIssnException {
        handler.handleRequest(dataGenerator.deletePublishedResourceEvent(), output, context);
        String response = output.toString();
        verifyRestHighLevelClientInvokedOnRemove();
        assertThat(response, containsString(DELETE.toString()));
    }

    @Test
    void handlerDoesNotSendRequestToElasticSearchWhenResourceInNotPublished() throws IOException, InvalidIssnException {
        InputStream event = dataGenerator.createResourceEvent(MODIFY, DRAFT, DRAFT);
        handler.handleRequest(event, output, context);
        String response = output.toString();
        verifyRestClientIsNotInvoked();
        assertThat(response, containsString(NO_ACTION.toString()));
    }

    @ParameterizedTest(name = "handler throws exception when eventType is not valid:{0}")
    @NullAndEmptySource
    @ValueSource(strings = {UNKNOWN_EVENT})
    void handleRequestThrowsExceptionWhenInputIsUnknownEventName(String eventType)
        throws InvalidIssnException, MalformedURLException, JsonProcessingException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        InputStream input = dataGenerator.createResourceEvent(eventType, PUBLISHED, PUBLISHED);
        Executable action = () -> handler.handleRequest(input, output, context);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);
        assertThat(exception.getMessage(), containsString(PublicationUpdateEventHandler.UNKNOWN_OPERATION_ERROR));
        String eventTypeStringRepresentation = String.format("%s", eventType);

        assertThat(exception.getMessage(), containsString(eventTypeStringRepresentation));
        assertThat(appender.getMessages(), containsString(exception.getMessage()));
    }

    @ParameterizedTest(name = "handler invokes elastic search client when event is valid and is: {0}")
    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    void handlerInvokesElasticSearchClientWhenEventTypeIsValid(String eventType)
        throws IOException, InvalidIssnException {
        InputStream input = dataGenerator.createResourceEvent(eventType, PUBLISHED, PUBLISHED);

        handler.handleRequest(input, output, context);
        String response = output.toString();
        verifyRestHighLevelClientInvocation(eventType);

        IndexAction expectedIndexAction = REMOVE.equals(eventType) ? DELETE : INDEX;
        assertThat(response, containsString(expectedIndexAction.toString()));
    }

    @Test
    void handleRequestThrowsExceptionWhenInputDoesNotIncludeNewOrOldImage() {
        String inputString = dataGenerator.createEmptyEvent();
        InputStream inputStream = IoUtils.stringToStream(inputString);

        Executable action = () -> handler.handleRequest(inputStream, output, context);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action);

        assertThat(exception.getMessage(), containsString(INVALID_EVENT_ERROR));

        String inputStringWithoutWhitespaces = StringUtils.removeWhiteSpaces(inputString);
        String exceptionMessagesWithoutWhitespaces = StringUtils.removeWhiteSpaces(exception.getMessage());
        assertThat(exceptionMessagesWithoutWhitespaces, containsString(inputStringWithoutWhitespaces));

        String appenderMessagesWithoutSpaces = removeAllWhiteSpaces(testAppender.getMessages());
        assertThat(testAppender.getMessages(), containsString(INVALID_EVENT_ERROR));
        assertThat(appenderMessagesWithoutSpaces, containsString(inputStringWithoutWhitespaces));
    }

    @ParameterizedTest(name = "handleRequest throws RuntimeException when rest client called with {0} throws "
                              + "exception")
    @ValueSource(strings = {INSERT, MODIFY, REMOVE})
    void handleRequestThrowsRuntimeExceptionWhenRestClientThrowsIoException(String eventName)
        throws IOException, InvalidIssnException {

        var expectedException = new IOException(RUNTIME_EXCEPTION_MESSAGE);
        setUpRestClientInError(eventName, expectedException);

        var elasticSearchRestClient = new ElasticSearchHighLevelRestClient(environment, restClient);

        handler = new PublicationUpdateEventHandler(elasticSearchRestClient);

        InputStream input = dataGenerator.createResourceEvent(eventName, PUBLISHED, PUBLISHED);
        Executable executable = () -> handler.handleRequest(input, output, context);
        var exception = assertThrows(RuntimeException.class, executable);

        assertThat(exception.getMessage(), containsString(expectedException.getMessage()));
    }

    @Test
    void publicationUpdateEventHandlerCreatesHttpRequestWithIndexDocumentWithModifyEventValidRecord()
        throws IOException, InvalidIssnException {
        InputStream input = dataGenerator.createResourceEvent(MODIFY, PUBLISHED, PUBLISHED);
        handler.handleRequest(input, output, context);
        assertThat(output.toString(), containsString(INDEX.toString()));
    }

    @ParameterizedTest(name = "handler ignores resources that are not published. Checking status: {0}")
    @NullSource
    @EnumSource(value = PublicationStatus.class, names = {"DRAFT", "DRAFT_FOR_DELETION"})
    void handlerIgnoresResourcesThatAreNotPublished(PublicationStatus publicationStatus)
        throws InvalidIssnException, IOException {
        InputStream input = dataGenerator.createResourceEvent(MODIFY, publicationStatus, publicationStatus);
        String resourceIdentifier = dataGenerator.getNewPublication().getIdentifier().toString();
        handler.handleRequest(input, output, context);
        verifyRestClientIsNotInvoked();

        assertThat(output.toString(), containsString(NO_ACTION.toString()));
        assertThat(testAppender.getMessages(), containsString(RESOURCE_IS_NOT_PUBLISHED_WARNING));
        assertThat(testAppender.getMessages(), containsString(resourceIdentifier));
    }

    @Test
    void handlerIndexesPublicationThatChangedStatusToPublished()
        throws InvalidIssnException, IOException {
        InputStream input = dataGenerator.createResourceEvent(MODIFY, DRAFT, PUBLISHED);
        handler.handleRequest(input, output, context);
        verifyRestHighLevelClientInvocation(MODIFY);
        assertThat(output.toString(), containsString(INDEX.toString()));
    }

    @Test
    void handlerDeletesFromIndexPublicationThatChangedStatusFromPublishedToSomethingElse()
        throws InvalidIssnException, IOException {
        InputStream input = dataGenerator.createResourceEvent(MODIFY, PUBLISHED, DRAFT);
        String resourceIdentifier = dataGenerator.getNewPublication().getIdentifier().toString();
        handler.handleRequest(input, output, context);
        verifyRestHighLevelClientInvocation(REMOVE);
        assertThat(output.toString(), containsString(DELETE.toString()));
        assertThat(testAppender.getMessages(),containsString(REMOVING_RESOURCE_WARNING));
        assertThat(testAppender.getMessages(),containsString(resourceIdentifier));
    }

    @Test
    void handlerIgnoresResourcesThatAreNotPublished()
        throws InvalidIssnException, IOException {
        InputStream input = dataGenerator.createResourceEvent(MODIFY, null, null);
        handler.handleRequest(input, output, context);
        verifyRestClientIsNotInvoked();
        assertThat(output.toString(), containsString(NO_ACTION.toString()));
    }

    private String removeAllWhiteSpaces(String stringWithSpaces) {
        return Try.of(stringWithSpaces)
                   .map(StringUtils::replaceWhiteSpacesWithSpace)
                   .map(StringUtils::removeWhiteSpaces)
                   .orElseThrow();
    }

    private SearchResourcesResponse searchElasticSearch(String term) throws ApiGatewayException {
        return elasticSearchRestClient.searchSingleTerm(term, SINGLE_RESULT, FIRST_RESULT, "id",
                                                        SortOrder.DESC);
    }

    private ElasticSearchHighLevelRestClient createHighLevelClientConnectedToLocalhost() throws IOException {
        restClient = clientToLocalInstance();
        ElasticSearchHighLevelRestClient esClient = new ElasticSearchHighLevelRestClient(environment, restClient);
        handler = new PublicationUpdateEventHandler(esClient);
        return esClient;
    }

    private void createIndex(RestHighLevelClientWrapper client) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(ELASTICSEARCH_ENDPOINT_INDEX);
        createIndexRequest.settings(Settings.builder()
                                        .put("index.number_of_shards", 1)
                                        .put("index.number_of_replicas", 1)
        );

        String mapping = customFieldTypeMappingToMakeIdsSortable();
        createIndexRequest.mapping(mapping, XContentType.JSON);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    private String customFieldTypeMappingToMakeIdsSortable() {
        return IoUtils.stringFromResources(Path.of("fields_mapping.json"));
    }

    private IndexDocument getSingleHit(SearchResourcesResponse result) {
        return result.getHits()
                   .stream()
                   .map(this::parseJson)
                   .collect(SingletonCollector.collect());
    }

    private IndexDocument parseJson(JsonNode json) {

        return JsonUtils.objectMapper.convertValue(json, IndexDocument.class);
    }

    private void waitForDocumentToBecomeAvailable() {
        try {
            Thread.sleep(TIME_FOR_DOCUMENT_TO_BECOME_AVAILABLE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private RestHighLevelClientWrapper mockElasticSearch() throws IOException {
        RestHighLevelClientWrapper client = mock(RestHighLevelClientWrapper.class);
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

    private void setUpRestClientInError(String eventName, Exception expectedException) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            when(restClient.index(any(), any())).thenThrow(expectedException);
        } else {
            when(restClient.delete(any(), any())).thenThrow(expectedException);
        }
    }

    private void verifyRestHighLevelClientInvocation(String eventName) throws IOException {
        if (UPSERT_EVENTS.contains(eventName)) {
            verify(restClient, (atMostOnce())).index(any(), any());
            verify(restClient, (times(1))).index(any(), any());
        } else {
            verify(restClient, (times(1))).delete(any(), any());
        }
    }

    private void verifyRestHighLevelClientInvokedOnRemove() throws IOException {
        verifyRestHighLevelClientInvocation(REMOVE);
    }
}
