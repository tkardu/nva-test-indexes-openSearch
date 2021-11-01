package no.unit.nva.indexing.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.events.handlers.DestinationsEventBridgeEventHandler;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UnixPath;
import nva.commons.core.paths.UriWrapper;

public class IndexResourceHandler extends DestinationsEventBridgeEventHandler<IndexResourceEvent, Void> {

    private static final String EVENT_BUCKET_NAME = IndexingConfig.ENVIRONMENT.readEnv("EVENT_BUCKET_NAME");

    private final S3Driver s3Driver;
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;

    @JacocoGenerated
    public IndexResourceHandler() {
        this(S3Driver.fromPermanentCredentialsInEnvironment(EVENT_BUCKET_NAME), IndexingConstants.defaultEsClient());
    }

    public IndexResourceHandler(S3Driver s3Driver, ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        super(IndexResourceEvent.class);
        this.s3Driver = s3Driver;
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    @Override
    protected Void processInputPayload(IndexResourceEvent input, AwsEventBridgeEvent<AwsEventBridgeDetail<IndexResourceEvent>> event, Context context) {

        UnixPath resourceRelativePath = new UriWrapper(input.getResourceLocation()).toS3bucketPath();
        String resource = s3Driver.getFile(resourceRelativePath);
        IndexResourceWrapper indexResourceWrapper = new IndexResourceWrapper(resource);

        try {
            elasticSearchRestClient.addDocumentToIndex(indexResourceWrapper.toIndexRequest());
        } catch (SearchException e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
