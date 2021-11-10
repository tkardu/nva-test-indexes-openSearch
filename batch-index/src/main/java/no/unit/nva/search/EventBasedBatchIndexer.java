package no.unit.nva.search;

import static no.unit.nva.search.BatchIndexingConstants.NUMBER_OF_FILES_PER_EVENT;
import static no.unit.nva.search.BatchIndexingConstants.defaultEsClient;
import static no.unit.nva.search.BatchIndexingConstants.defaultEventBridgeClient;
import static no.unit.nva.search.BatchIndexingConstants.defaultS3Client;
import static no.unit.nva.search.EmitEventUtils.emitEvent;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.InputStream;
import java.io.OutputStream;
import no.unit.nva.events.handlers.EventHandler;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.identifiers.SortableIdentifier;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.s3.S3Client;

public class EventBasedBatchIndexer extends EventHandler<ImportDataRequest, SortableIdentifier[]> {

    private static final Logger logger = LoggerFactory.getLogger(EventBasedBatchIndexer.class);
    private final S3Client s3Client;
    private final ElasticSearchHighLevelRestClient elasticSearchClient;
    private final EventBridgeClient eventBridgeClient;
    private final int numberOfFilesPerEvent;

    @JacocoGenerated
    public EventBasedBatchIndexer() {
        this(defaultS3Client(), defaultEsClient(), defaultEventBridgeClient(), NUMBER_OF_FILES_PER_EVENT);
    }

    protected EventBasedBatchIndexer(S3Client s3Client,
                                     ElasticSearchHighLevelRestClient elasticSearchClient,
                                     EventBridgeClient eventBridgeClient,
                                     int numberOfFilesPerEvent
    ) {
        super(ImportDataRequest.class);
        this.s3Client = s3Client;
        this.elasticSearchClient = elasticSearchClient;
        this.eventBridgeClient = eventBridgeClient;
        this.numberOfFilesPerEvent = numberOfFilesPerEvent;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        String inputString = IoUtils.streamToString(inputStream);
        logger.info(inputString);
        super.handleRequest(IoUtils.stringToStream(inputString), outputStream, context);
    }

    @Override
    protected SortableIdentifier[] processInput(ImportDataRequest input, AwsEventBridgeEvent<ImportDataRequest> event,
                                                Context context) {
        logger.info("Indexing folder:" + input.getS3Location());
        logger.info("Indexing startingPoint:" + input.getStartMarker());
        IndexingResult<SortableIdentifier> result = new BatchIndexer(input, s3Client,
                                                                     elasticSearchClient,
                                                                     numberOfFilesPerEvent
                                                                     ).processRequest();
        if (result.isTruncated() && BatchIndexingConstants.RECURSION_ENABLED) {
            emitEventToProcessNextBatch(input, context, result);
        }
        return result.getFailedResults().toArray(SortableIdentifier[]::new);
    }

    private void emitEventToProcessNextBatch(ImportDataRequest input, Context context,
                                             IndexingResult<SortableIdentifier> result) {
        ImportDataRequest newImportDataRequest =
            new ImportDataRequest(input.getS3Location(), result.getNextStartMarker());
        emitEvent(eventBridgeClient, newImportDataRequest, context);
    }
}
