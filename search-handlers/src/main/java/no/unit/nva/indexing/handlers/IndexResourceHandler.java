package no.unit.nva.indexing.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import no.unit.nva.events.handlers.DestinationsEventBridgeEventHandler;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.IndexingClient;
import no.unit.nva.search.IndexingConfig;
import no.unit.nva.search.exception.SearchException;
import no.unit.nva.search.models.IndexEvent;
import no.unit.nva.search.models.IndexDocument;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.paths.UnixPath;
import nva.commons.core.paths.UriWrapper;

public class IndexResourceHandler extends DestinationsEventBridgeEventHandler<IndexEvent, Void> {

    private static final String EXPANDED_RESOURCES_BUCKET = IndexingConfig.ENVIRONMENT.readEnv(
        "EXPANDED_RESOURCES_BUCKET");

    private final S3Driver s3Driver;
    private final IndexingClient indexingClient;

    @JacocoGenerated
    public IndexResourceHandler() {
        this(new S3Driver(EXPANDED_RESOURCES_BUCKET), defaultIndexingClient());
    }

    public IndexResourceHandler(S3Driver s3Driver, IndexingClient indexingClient) {
        super(IndexEvent.class);
        this.s3Driver = s3Driver;
        this.indexingClient = indexingClient;
    }

    @JacocoGenerated
    public static IndexingClient defaultIndexingClient() {
        return new IndexingClient();
    }

    @Override
    protected Void processInputPayload(IndexEvent input, AwsEventBridgeEvent<AwsEventBridgeDetail<IndexEvent>> event,
                                       Context context) {

        UnixPath resourceRelativePath = new UriWrapper(input.getUri()).toS3bucketPath();
        String resource = s3Driver.getFile(resourceRelativePath);
        IndexDocument indexDocument = IndexDocument.fromJsonString(resource);
        try {
            indexingClient.addDocumentToIndex(indexDocument);
        } catch (SearchException e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
