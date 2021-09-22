package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import java.util.List;

import no.unit.nva.events.handlers.EventHandler;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.publication.s3imports.ApplicationConstants;
import nva.commons.core.JacocoGenerated;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

public class EventBasedBatchIndexer extends EventHandler<ImportDataRequest, String[]> {

    private final S3Client s3Client;
    private final ElasticSearchHighLevelRestClient elasticSearchClient;

    @JacocoGenerated
    public EventBasedBatchIndexer() {
        this(defaultS3Client(), defaultEsClient());
    }

    protected EventBasedBatchIndexer(S3Client s3Client,
                                     ElasticSearchHighLevelRestClient elasticSearchClient) {
        super(ImportDataRequest.class);
        this.s3Client = s3Client;
        this.elasticSearchClient = elasticSearchClient;
    }

    @JacocoGenerated
    public static S3Client defaultS3Client() {
        return S3Client.builder()
            .region(ApplicationConstants.AWS_REGION)
            .httpClient(UrlConnectionHttpClient.create())
            .build();
    }

    @Override
    protected String[] processInput(ImportDataRequest input, AwsEventBridgeEvent<ImportDataRequest> event,
                                    Context context) {
        List<String> result = new BatchIndexer(input, s3Client, elasticSearchClient).processRequest();
        return result.toArray(String[]::new);
    }

    @JacocoGenerated
    private static ElasticSearchHighLevelRestClient defaultEsClient() {
        return new ElasticSearchHighLevelRestClient();
    }
}
