package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static no.unit.nva.search.ImportToSearchIndexHandlerTest.ELASTICSEARCH_ENDPOINT_ADDRESS;
import static no.unit.nva.search.ImportToSearchIndexHandlerTest.ELASTICSEARCH_ENDPOINT_INDEX;
import static nva.commons.apigateway.ApiGatewayHandler.ALLOWED_ORIGIN_ENV;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PrepareIndexForBatchInsertHandlerTest {

    private PrepareIndexForBatchInsertHandler handler;
    private ByteArrayOutputStream outputStream;
    private ElasticSearchHighLevelRestClient es;

    @BeforeEach
    public void init() throws IOException {
        outputStream= new ByteArrayOutputStream();
        Environment environment= setupMockEnvironment();
        RestHighLevelClient restEs = mock(RestHighLevelClient.class);
        IndicesClient indicesClient=  mock(IndicesClient.class);
        GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);
        when(getIndexResponse.getIndices()).thenReturn(new String[]{"resources"});
        when(restEs.indices()).thenReturn(indicesClient);
        when(indicesClient.get(any(GetIndexRequest.class),any(RequestOptions.class)))
            .thenReturn(getIndexResponse);
        es = new ElasticSearchHighLevelRestClient(environment, restEs);

        handler= new PrepareIndexForBatchInsertHandler(environment,es);
    }

    @Test
    public void handleRequestReturnsOKHandlerIsCalled() throws IOException {
        InputStream input = new HandlerRequestBuilder<Void>(JsonUtils.objectMapperNoEmpty)
                                .withBody((Void) null)
                                .build();
        handler.handleRequest(input,outputStream,mock(Context.class));
        GatewayResponse<Void> response = GatewayResponse.fromOutputStream(outputStream);
        assertThat(response.getStatusCode(),is(equalTo(HttpURLConnection.HTTP_OK)));

    }

    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        doReturn("*").when(environment)
            .readEnv(ALLOWED_ORIGIN_ENV);
        return environment;
    }


}