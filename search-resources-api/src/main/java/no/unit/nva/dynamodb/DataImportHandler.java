package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.exception.ImportException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.handlers.ApiGatewayHandler;
import nva.commons.handlers.RequestInfo;
import nva.commons.handlers.RestRequestHandler;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import org.apache.http.HttpStatus;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataImportHandler extends ApiGatewayHandler<ImportDataRequest, Void> {

    public static final String AWS_REGION_KEY = "ELASTICSEARCH_REGION";
    private final DynamoDBExportFileReader dynamoDBExportFileReader;

    @JacocoGenerated
    public DataImportHandler() {
        this(new Environment());
    }

    /**
     * Creating DataImportHandler from given environment.
     * @param environment for handler to operate in
     */
    public DataImportHandler(Environment environment) {
        this(environment, new ElasticSearchHighLevelRestClient(environment),
                AmazonS3ClientBuilder.standard()
                .withRegion(environment.readEnv(AWS_REGION_KEY))
                .build());
    }

    /**
     * Creating DataImportHandler capable or reading dynamodb json datafiles on S3.
     * @param environment settings for application
     * @param elasticSearchClient Client speaking with elasticSearch
     * @param s3Client Client speaking with Amazon S3
     *
     */
    public DataImportHandler(Environment environment,
                             ElasticSearchHighLevelRestClient elasticSearchClient,
                             AmazonS3 s3Client) {
        super(ImportDataRequest.class, environment, LoggerFactory.getLogger(DataImportHandler.class));
        this.dynamoDBExportFileReader = new DynamoDBExportFileReader(elasticSearchClient, s3Client);
    }

    /**
     * Implements the main logic of the handler. Any exception thrown by this method will be handled by {@link
     * RestRequestHandler#handleExpectedException} method.
     *
     * @param importDataRequest       The input object to the method. Usually a deserialized json.
     * @param requestInfo Request headers and path.
     * @param context     the ApiGateway context.ucket
     * @return the Response body that is going to be serialized in json
     * @throws ApiGatewayException all exceptions are caught by writeFailure and mapped to error codes through the
     *                             method {@link RestRequestHandler#getFailureStatusCode}
     */
    @Override
    protected Void processInput(ImportDataRequest importDataRequest, RequestInfo requestInfo, Context context)
            throws ApiGatewayException {

        try {
            dynamoDBExportFileReader.scanS3Folder(importDataRequest);
        } catch (IOException e) {
            logger.error("",e);
            throw new ImportException(e.getMessage());
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
    protected Integer getSuccessStatusCode(ImportDataRequest input, Void output) {
        return HttpStatus.SC_ACCEPTED;
    }
}
