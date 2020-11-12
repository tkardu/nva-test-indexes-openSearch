package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.RequestInfo;
import nva.commons.utils.Environment;
import nva.commons.utils.log.LogUtils;
import nva.commons.utils.log.TestAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataImportHandlerTest {

    private static final String SAMPLE_BUCKET_NAME = "nva-datapipeline";
    private static final String SAMPLE_S3FOLDER_KEY = "2020-10-12-06-55-32";

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";


    private DataImportHandler  handler;
    private Context context;
    private Environment environment;
    private TestAppender testAppender;


    /**
     * Set up test environment.
     */
    @BeforeEach
    void init() {
        context = mock(Context.class);
        environment = setupMockEnvironment();



        ElasticSearchHighLevelRestClient mockElasticSearchClient = mock(ElasticSearchHighLevelRestClient.class);
        AmazonS3 mockS3Client = mock(AmazonS3.class);
        ListObjectsV2Result listing = mock(ListObjectsV2Result.class);
        when(mockS3Client.listObjectsV2(anyString(),anyString())).thenReturn(listing);
        handler = new DataImportHandler(environment, mockElasticSearchClient, mockS3Client);
        testAppender = LogUtils.getTestingAppender(DynamoDBStreamHandler.class);
    }

    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
                .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
                .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        return environment;
    }


    @Test
    void handlerAcceptsSampleImportRequest() throws ApiGatewayException {

        ImportDataRequest importDataRequest = new ImportDataRequest.Builder()
                .withS3Bucket(SAMPLE_BUCKET_NAME)
                .withS3FolderKey(SAMPLE_S3FOLDER_KEY)
                .build();

        handler.processInput(importDataRequest,new RequestInfo(),  context);
    }

    @Test
    void handlerThrowsExceptionWhenInputIsBad()  {
        Executable executable = () ->  handler.processInput(null, null,  context);
        assertThrows(ApiGatewayException.class, executable);
    }

}