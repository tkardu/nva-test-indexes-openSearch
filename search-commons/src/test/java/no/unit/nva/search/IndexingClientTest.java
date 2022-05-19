package no.unit.nva.search;

import static no.unit.nva.commons.json.JsonUtils.dtoObjectMapper;
import static no.unit.nva.search.IndexingClient.BULK_SIZE;
import static no.unit.nva.search.IndexingConfig.objectMapper;
import static no.unit.nva.testutils.RandomDataGenerator.randomJson;
import static no.unit.nva.testutils.RandomDataGenerator.randomString;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.models.EventConsumptionAttributes;
import no.unit.nva.search.models.IndexDocument;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class IndexingClientTest {

    public static final int SET_OF_RESOURCES_THAT_DO_NOT_FIT_EXACTLY_IN_THE_BULK_SIZE_OF_A_BULK_REQUEST = 1256;
    public static final IndexResponse UNUSED_INDEX_RESPONSE = null;
    private RestHighLevelClientWrapper esClient;
    private IndexingClient indexingClient;
    private AtomicReference<IndexRequest> submittedIndexRequest;

    @BeforeEach
    public void init() throws IOException {
        esClient = setupMockEsClient();
        indexingClient = new IndexingClient(esClient);
        submittedIndexRequest = new AtomicReference<>();
    }

    @Test
    void shouldCreateDefaultObjectWithoutFailing() {
        assertDoesNotThrow((Executable) IndexingClient::new);
    }

    @Test
    void shouldIndexAllDocumentsInBatchInBulksOfSpecifiedSize() throws IOException {
        var indexDocuments =
            IntStream.range(0, SET_OF_RESOURCES_THAT_DO_NOT_FIT_EXACTLY_IN_THE_BULK_SIZE_OF_A_BULK_REQUEST)
                .boxed()
                .map(i -> randomJson())
                .map(this::toIndexDocument)
                .collect(Collectors.toList());
        List<BulkResponse> provokeExecution = indexingClient.batchInsert(indexDocuments.stream())
            .collect(Collectors.toList());
        assertThat(provokeExecution, is(not(nullValue())));

        int expectedNumberOfBulkRequests = (int) Math.ceil(((double) indexDocuments.size()) / ((double) BULK_SIZE));
        verify(esClient, times(expectedNumberOfBulkRequests))
            .bulk(any(BulkRequest.class), any(RequestOptions.class));
    }

    @Test
    void shouldSendIndexRequestWithIndexNameSpecifiedByIndexDocument() throws IOException {
        var indexDocument = sampleIndexDocument();
        var expectedIndex = indexDocument.getConsumptionAttributes().getIndex();
        indexingClient.addDocumentToIndex(indexDocument);

        assertThat(submittedIndexRequest.get().index(), is(equalTo(expectedIndex)));
        assertThat(extractDocumentFromSubmittedIndexRequest(), is(equalTo(indexDocument.getResource())));
    }

    @Test
    void shouldVerifyIndicesClientCreatedToAccessIndicesAPI() {
        IndicesClientWrapper expectedIndicesClientWrapper = createMockIndicesClientWrapper();
        when(esClient.indices()).thenReturn(expectedIndicesClientWrapper);
        var actualIndicesClientWrapper = indexingClient.getIndicesClientWrapper();
        assertEquals(expectedIndicesClientWrapper, actualIndicesClientWrapper);
    }

    @Test
    void shouldVerifyProperIndexCreateRequestSentToIndicesClient() throws IOException {
        IndicesClient indicesClient = mock(IndicesClient.class);
        var indicesClientWrapper = new IndicesClientWrapper(indicesClient);
        indexingClient.createIndexBasedOnName(randomString(), indicesClientWrapper);
        var expectedNumberOfCreateInvocationsToEs = 1;
        verify(indicesClient, times(expectedNumberOfCreateInvocationsToEs)).create(any(CreateIndexRequest.class),
                                                                                   any(RequestOptions.class));
    }

    @Test
    void shouldThrowExceptionWhenIndicesWrapperFailedToCreateIndex() throws IOException {
        var expectedErrorMessage = randomString();
        var indicesClientWrapper = createMockIndicesClientWrapper();
        when(indicesClientWrapper.create(any(CreateIndexRequest.class), any(RequestOptions.class))).thenThrow(
            new IOException(expectedErrorMessage));
        Executable createIndexAction = () -> indexingClient.createIndexBasedOnName(randomString(),
                                                                                   indicesClientWrapper);
        var actualException = assertThrows(IOException.class, createIndexAction);
        assertThat(actualException.getMessage(), containsString(expectedErrorMessage));
    }

    @Test
    void shouldThrowExceptionContainingTheCauseWhenIndexDocumentFailsToBeIndexed() throws IOException {
        String expectedMessage = randomString();
        esClient = mock(RestHighLevelClientWrapper.class);
        when(esClient.index(any(IndexRequest.class), any(RequestOptions.class)))
            .thenThrow(new IOException(expectedMessage));

        indexingClient = new IndexingClient(esClient);

        Executable indexingAction = () -> indexingClient.addDocumentToIndex(sampleIndexDocument());
        var exception = assertThrows(IOException.class, indexingAction);
        assertThat(exception.getMessage(), containsString(expectedMessage));
    }

    @Test
    void shouldNotThrowExceptionWhenTryingToDeleteNonExistingDocument() throws IOException {
        RestHighLevelClientWrapper restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        DeleteResponse nothingFoundResponse = mock(DeleteResponse.class);
        when(nothingFoundResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
        when(restHighLevelClient.delete(any(), any())).thenReturn(nothingFoundResponse);
        IndexingClient indexingClient = new IndexingClient(restHighLevelClient);
        assertDoesNotThrow(() -> indexingClient.removeDocumentFromIndex("1234"));
    }

    @NotNull
    private IndicesClientWrapper createMockIndicesClientWrapper() {
        IndicesClient indicesClient = mock(IndicesClient.class);
        return new IndicesClientWrapper(indicesClient);
    }

    private RestHighLevelClientWrapper setupMockEsClient() throws IOException {
        var esClient = mock(RestHighLevelClientWrapper.class);
        when(esClient.index(any(IndexRequest.class), any(RequestOptions.class)))
            .thenAnswer(invocation -> {
                var indexRequest = (IndexRequest) invocation.getArgument(0);
                submittedIndexRequest.set(indexRequest);
                return UNUSED_INDEX_RESPONSE;
            });
        return esClient;
    }

    private IndexDocument sampleIndexDocument() {
        EventConsumptionAttributes consumptionAttributes =
            new EventConsumptionAttributes(randomString(), SortableIdentifier.next());
        return new IndexDocument(consumptionAttributes, sampleJsonObject());
    }

    private JsonNode sampleJsonObject() {
        return attempt(() -> dtoObjectMapper.readTree(randomJson())).orElseThrow();
    }

    private IndexDocument toIndexDocument(String jsonString) {
        var consumptionAttributes = new EventConsumptionAttributes(randomString(), SortableIdentifier.next());
        var json = attempt(() -> objectMapper.readTree(jsonString)).orElseThrow();
        return new IndexDocument(consumptionAttributes, json);
    }

    private JsonNode extractDocumentFromSubmittedIndexRequest() throws JsonProcessingException {
        return objectMapper.readTree(submittedIndexRequest.get().source().toBytesRef().utf8ToString());
    }
}
