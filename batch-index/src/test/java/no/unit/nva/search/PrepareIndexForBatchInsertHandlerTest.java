package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.FIFTEEN_MINUTES;
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
    private Environment environment;
    private CreateIndexRequest receivedCreateIndexRequest;
    private UpdateSettingsRequest receivedUpdateSettingsRequest;

    @BeforeEach
    public void init() {
        outputStream = new ByteArrayOutputStream();
        environment = new Environment();
        receivedCreateIndexRequest = null;
        this.handler = newHandler(new IndicesClientMock());
    }

    @Test
    public void handleRequestReturnsOkWhenHandlerIsCalled() throws IOException {
        InputStream input = defaultRequest();
        handler.handleRequest(input, outputStream, mock(Context.class));
        GatewayResponse<Void> response = GatewayResponse.fromOutputStream(outputStream);
        assertThat(response.getStatusCode(), is(equalTo(HttpURLConnection.HTTP_OK)));
    }

    @Test
    public void handleRequestUpdateRefreshIntervalWhenHandlerIsCalledAndIndexAlreadyExists() throws IOException {
        InputStream input = defaultRequest();
        IndicesClientMock spiedIndicesClient = spy(new IndicesClientMock());
        handler = newHandler(spiedIndicesClient);

        handler.handleRequest(input, outputStream, mock(Context.class));
        verify(spiedIndicesClient, times(1)).putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class));

        Settings indexSettings = receivedUpdateSettingsRequest.settings();
        assertThat(indexSettings.get(ELASTIC_SEARCH_INDEX_REFRESH_INTERVAL), is(equalTo(FIFTEEN_MINUTES)));
    }

    @Test
    public void handleRequestCreatesIndexWithSpecifiedRefreshIntervalWhenHandlerIsCalledAndIndexDoesNotExist()
        throws IOException {
        InputStream input = defaultRequest();
        IndicesClientMock spiedIndicesClient = spy(indicesClientWithoutAnyIndex());
        handler = newHandler(spiedIndicesClient);

        handler.handleRequest(input, outputStream, mock(Context.class));
        verify(spiedIndicesClient, times(1)).create(any(CreateIndexRequest.class), any(RequestOptions.class));
        Settings indexSettings = receivedCreateIndexRequest.settings();
        assertThat(indexSettings.get(ELASTIC_SEARCH_INDEX_REFRESH_INTERVAL), is(equalTo(FIFTEEN_MINUTES)));
    }

    protected void setReceivedUpdateSettingsRequest(
        UpdateSettingsRequest receivedUpdateSettingsRequest) {
        this.receivedUpdateSettingsRequest = receivedUpdateSettingsRequest;
    }

    protected void setReceivedCreateIndexRequest(CreateIndexRequest createIndexRequest) {
        this.receivedCreateIndexRequest = createIndexRequest;
    }

    private PrepareIndexForBatchInsertHandler newHandler(IndicesClientWrapper indicesClient) {
        RestHighLevelClientWrapper elasticSearchClient = newElasticSearchClientWrapper(indicesClient);
        ElasticSearchHighLevelRestClient es = new ElasticSearchHighLevelRestClient(elasticSearchClient);
        return new PrepareIndexForBatchInsertHandler(environment, es);
    }

    private IndicesClientMock indicesClientWithoutAnyIndex() {
        return new IndicesClientMock() {
            @Override
            public GetIndexResponse get(GetIndexRequest getIndexRequest, RequestOptions requestOptions) {
                GetIndexResponse response = mock(GetIndexResponse.class);
                when(response.getIndices()).thenReturn(new String[0]);
                return response;
            }
        };
    }

    private RestHighLevelClientWrapper newElasticSearchClientWrapper(IndicesClientWrapper indicesClient) {
        RestHighLevelClientWrapper elasticSearchClient = mock(RestHighLevelClientWrapper.class);
        when(elasticSearchClient.indices()).thenReturn(indicesClient);
        return elasticSearchClient;
    }

    private InputStream defaultRequest() throws com.fasterxml.jackson.core.JsonProcessingException {
        return new HandlerRequestBuilder<Void>(JsonUtils.objectMapperNoEmpty)
                   .withBody(null)
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
                                                RequestOptions requestOptions) {
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
