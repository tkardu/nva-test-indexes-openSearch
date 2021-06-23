package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.TEN_MINUTES;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTIC_SEARCH_INDEX_REFRESH_INTERVAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import no.unit.nva.testutils.HandlerRequestBuilder;
import nva.commons.apigateway.GatewayResponse;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PrepareIndexForBatchInsertHandlerTest {

    private PrepareIndexForBatchInsertHandler handler;
    private ByteArrayOutputStream outputStream;
    private UpdateSettingsRequest receivedUpdateSettingsRequest;
    private IndicesClientMock indicesClient;
    private Environment environment;
    private CreateIndexRequest receivedCreateIndexRequest;

    @BeforeEach
    public void init() throws IOException {
        outputStream = new ByteArrayOutputStream();
        environment = new Environment();
        receivedUpdateSettingsRequest = null;
        receivedCreateIndexRequest = null;
        indicesClient = new IndicesClientMock();
        RestHighLevelClientWrapper elasticSearchClient = newElasticSearchClientWrapper(indicesClient);
        ElasticSearchHighLevelRestClient es = new ElasticSearchHighLevelRestClient(elasticSearchClient);

        handler = new PrepareIndexForBatchInsertHandler(environment, es);
    }

    @Test
    public void handleRequestReturnsOKHandlerIsCalled() throws IOException {
        InputStream input = defaultRequest();
        handler.handleRequest(input, outputStream, mock(Context.class));
        GatewayResponse<Void> response = GatewayResponse.fromOutputStream(outputStream);
        assertThat(response.getStatusCode(), is(equalTo(HttpURLConnection.HTTP_OK)));
    }

    @Test
    public void handleRequestSetsRefreshRateTo10MinutesWhenHandlerIsCalledAndIndexAlreadyExists() throws IOException {
        InputStream input = defaultRequest();
        IndicesClientMock spiedIndicesClient = spy(indicesClient);
        RestHighLevelClientWrapper elasticSearchClient = newElasticSearchClientWrapper(spiedIndicesClient);
        ElasticSearchHighLevelRestClient es = new ElasticSearchHighLevelRestClient(elasticSearchClient);

        handler = new PrepareIndexForBatchInsertHandler(environment, es);
        handler.handleRequest(input, outputStream, mock(Context.class));
        verify(spiedIndicesClient, times(1)).putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class));
    }

    @Test
    public void handleRequestCreatesIndexWithRefreshRateTo10MinutesWhenHandlerIsCalledAndIndexDoesNotExist()
        throws IOException {
        InputStream input = defaultRequest();
        IndicesClientMock spiedIndicesClient = spy(indicesClient);
        RestHighLevelClientWrapper elasticSearchClient = newElasticSearchClientWrapper(spiedIndicesClient);
        ElasticSearchHighLevelRestClient es = new ElasticSearchHighLevelRestClient(elasticSearchClient);

        handler = new PrepareIndexForBatchInsertHandler(environment, es);
        handler.handleRequest(input, outputStream, mock(Context.class));
        verify(spiedIndicesClient, times(1)).create(any(CreateIndexRequest.class), any(RequestOptions.class));
        Settings indexSettings = receivedCreateIndexRequest.settings();
        assertThat(indexSettings.get(ELASTIC_SEARCH_INDEX_REFRESH_INTERVAL), is(equalTo(TEN_MINUTES)));
    }

    protected void setReceivedUpdateSettingsRequest(UpdateSettingsRequest updateSettingsRequest) {
        this.receivedUpdateSettingsRequest = updateSettingsRequest;
    }

    protected void setReceivedCreateIndexRequest(CreateIndexRequest createIndexRequest) {
        this.receivedCreateIndexRequest = createIndexRequest;
    }

    private RestHighLevelClientWrapper newElasticSearchClientWrapper(IndicesClientWrapper indicesClient) {
        RestHighLevelClientWrapper elasticSearchClient = mock(RestHighLevelClientWrapper.class);
        when(elasticSearchClient.indices()).thenReturn(indicesClient);
        return elasticSearchClient;
    }

    private InputStream defaultRequest() throws com.fasterxml.jackson.core.JsonProcessingException {
        return new HandlerRequestBuilder<Void>(JsonUtils.objectMapperNoEmpty)
                   .withBody((Void) null)
                   .build();
    }

    private class IndicesClientMock extends IndicesClientWrapper {

        public IndicesClientMock() {
            super(null);
        }

        public CreateIndexResponse create(CreateIndexRequest createIndexRequest, RequestOptions requestOptions) {

            setReceivedCreateIndexRequest(createIndexRequest);
            return new CreateIndexResponse(true, true, ELASTICSEARCH_ENDPOINT_INDEX);
        }

        @Override
        public AcknowledgedResponse putSettings(UpdateSettingsRequest updateSettingsRequest,
                                                RequestOptions requestOptions)
            throws IOException {
            setReceivedUpdateSettingsRequest(updateSettingsRequest);
            return new AcknowledgedResponse(true);
        }

        @Override
        public GetIndexResponse get(GetIndexRequest getIndexRequest, RequestOptions requestOptions) {
            GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);
            when(getIndexResponse.getIndices()).thenReturn(new String[]{ELASTICSEARCH_ENDPOINT_INDEX});
            return getIndexResponse;
        }
    }
}