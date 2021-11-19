package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

import java.time.Instant;

import static no.unit.nva.search.BatchIndexingConstants.BATCH_INDEX_EVENT_BUS_NAME;
import static no.unit.nva.search.BatchIndexingConstants.BATCH_INDEX_EVENT_TOPIC;

public final class EmitEventUtils {

    private static Logger logger = LoggerFactory.getLogger(EmitEventUtils.class);

    private EmitEventUtils() {

    }

    public static void emitEvent(EventBridgeClient eventBridgeClient,
                                 ImportDataRequestEvent importDataRequest,
                                 Context context) {
        PutEventsRequestEntry putEventRequestEntry = eventEntry(importDataRequest, context);
        logger.info("Event:" + putEventRequestEntry.toString());
        PutEventsRequest putEventRequest = PutEventsRequest.builder().entries(putEventRequestEntry).build();
        eventBridgeClient.putEvents(putEventRequest);
    }

    private static PutEventsRequestEntry eventEntry(ImportDataRequestEvent importDataRequest, Context context) {
        return PutEventsRequestEntry.builder()
            .eventBusName(BATCH_INDEX_EVENT_BUS_NAME)
            .source(EventBasedBatchIndexer.class.getName())
            .time(Instant.now())
            .detailType(BATCH_INDEX_EVENT_TOPIC)
            .detail(importDataRequest.toJsonString())
            .resources(context.getInvokedFunctionArn())
            .build();
    }
}
