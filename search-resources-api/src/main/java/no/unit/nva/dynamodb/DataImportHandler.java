package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.ApiGatewayHandler;
import nva.commons.handlers.RequestInfo;
import nva.commons.handlers.RestRequestHandler;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import org.apache.http.HttpStatus;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static no.unit.nva.search.RequestUtil.getS3Bucket;
import static no.unit.nva.search.RequestUtil.getS3FolderKey;

public class DataImportHandler extends ApiGatewayHandler<Void, Void> {

    private final DynamoDBExportFileReader dynamoDBExportFileReader;

    @JacocoGenerated
    public DataImportHandler() {
        this(new Environment());
    }

    public DataImportHandler(Environment environment) {
        this(environment, new ElasticSearchHighLevelRestClient(environment));
    }

    /**
     * Creating DataImportHandler capable or reading dynamodb json datafiles on S3.
     * @param environment settings for application
     * @param elasticSearchClient Client speaking with elasticSearch
     */
    public DataImportHandler(Environment environment, ElasticSearchHighLevelRestClient elasticSearchClient) {
        super(Void.class, environment, LoggerFactory.getLogger(DataImportHandler.class));
        this.dynamoDBExportFileReader = new DynamoDBExportFileReader(elasticSearchClient);
    }

    /**
     * Implements the main logic of the handler. Any exception thrown by this method will be handled by {@link
     * RestRequestHandler#handleExpectedException} method.
     *
     * @param input       The input object to the method. Usually a deserialized json.
     * @param requestInfo Request headers and path.
     * @param context     the ApiGateway context.ucket
     * @return the Response body that is going to be serialized in json
     * @throws ApiGatewayException all exceptions are caught by writeFailure and mapped to error codes through the
     *                             method {@link RestRequestHandler#getFailureStatusCode}
     */
    @Override
    protected Void processInput(Void input, RequestInfo requestInfo, Context context) throws ApiGatewayException {

        String s3Bucket = getS3Bucket(requestInfo);
        String s3folderKey = getS3FolderKey(requestInfo);

        try {
            dynamoDBExportFileReader.scanS3Folder(s3Bucket,s3folderKey);
        } catch (IOException e) {
            logger.error("",e);
        }
        return null;
    }


    /**
     * Define the success status code.
     *
     * @param input  The request input.
     * @param output The response output
     * @return the success status code.
     */
    @Override
    protected Integer getSuccessStatusCode(Void input, Void output) {
        return HttpStatus.SC_ACCEPTED;
    }
}
