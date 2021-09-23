package no.unit.nva.search;

import static no.unit.nva.search.BatchIndexingConstants.BATCH_INDEX_EVENT_BUS_NAME;
import static no.unit.nva.search.BatchIndexingConstants.BATCH_INDEX_EVENT_DETAIL_TYPE;
import static no.unit.nva.search.BatchIndexingConstants.defaultEsClient;
import static no.unit.nva.search.BatchIndexingConstants.defaultEventBridgeClient;
import static no.unit.nva.search.BatchIndexingConstants.defaultS3Client;
import com.amazonaws.services.lambda.runtime.Context;
import java.time.Instant;
import no.unit.nva.events.handlers.EventHandler;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import nva.commons.core.JacocoGenerated;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3Client;

public class EventBasedBatchIndexer extends EventHandler<ImportDataRequest, String[]> {

    private final S3Client s3Client;
    private final ElasticSearchHighLevelRestClient elasticSearchClient;
    private final EventBridgeClient eventBridgeClient;

    @JacocoGenerated
    public EventBasedBatchIndexer() {
        this(defaultS3Client(), defaultEsClient(), defaultEventBridgeClient());
    }

    protected EventBasedBatchIndexer(S3Client s3Client,
                                     ElasticSearchHighLevelRestClient elasticSearchClient,
                                     EventBridgeClient eventBridgeClient) {
        super(ImportDataRequest.class);
        this.s3Client = s3Client;
        this.elasticSearchClient = elasticSearchClient;
        this.eventBridgeClient = eventBridgeClient;
    }

    protected static void emitEvent(EventBridgeClient eventBridgeClient,
                                    ImportDataRequest importDataRequest,
                                    Context context) {
        PutEventsRequestEntry putEventRequestEntry = eventEntry(importDataRequest, context);
        PutEventsRequest putEventRequest = PutEventsRequest.builder().entries(putEventRequestEntry).build();
        eventBridgeClient.putEvents(putEventRequest);
    }

    protected static PutEventsRequestEntry eventEntry(ImportDataRequest importDataRequest, Context context) {
        return PutEventsRequestEntry.builder()
            .eventBusName(BATCH_INDEX_EVENT_BUS_NAME)
            .source(EventBasedBatchIndexer.class.getName())
            .time(Instant.now())
            .detailType(BATCH_INDEX_EVENT_DETAIL_TYPE)
            .detail(importDataRequest.toJsonString())
            .resources(context.getInvokedFunctionArn())
            .build();
    }

    @Override
    protected String[] processInput(ImportDataRequest input, AwsEventBridgeEvent<ImportDataRequest> event,
                                    Context context) {
        ProcessResult result = new BatchIndexer(input, s3Client, elasticSearchClient).processRequest();
        if (result.isTruncated()) {
            emitEventToProcessNextBatch(input, context, result);
        }
        return result.getFailedResults().toArray(String[]::new);
    }

    private void emitEventToProcessNextBatch(ImportDataRequest input, Context context, ProcessResult result) {
        ImportDataRequest newImportDataRequest =
            new ImportDataRequest(input.getS3Location(), result.getListingStartingPoint());
        emitEvent(eventBridgeClient, newImportDataRequest, context);
    }
}
