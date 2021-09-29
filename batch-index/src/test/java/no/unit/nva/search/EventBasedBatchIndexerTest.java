package no.unit.nva.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.testutils.IoUtils;
import nva.commons.core.JsonUtils;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class EventBasedBatchIndexerTest extends BatchIndexTest {

    EventBasedBatchIndexer indexer;
    private ByteArrayOutputStream outputStream;
    private StubElasticSearchHighLevelRestClient elasticSearchClient;
    private StubEventBridgeClient eventBridgeClient;

    @BeforeEach
    public void init() {
        this.outputStream = new ByteArrayOutputStream();
        elasticSearchClient = mockEsClient();
        eventBridgeClient = new StubEventBridgeClient();
        indexer = new EventBasedBatchIndexer(mockS3Client(), elasticSearchClient, eventBridgeClient);
    }

    @Test
    public void batchIndexerParsesEvent() {
        InputStream event = IoUtils.inputStreamFromResources("event.json");
        indexer.handleRequest(event,outputStream,CONTEXT);
    }

    @Test
    public void batchIndexerThrowsNoExceptionWhenValidInputIsSupplied() throws JsonProcessingException {
        ImportDataRequest initialEvent = initialEvent();
        final InputStream firstEvent = eventStream(initialEvent);
        assertDoesNotThrow(() -> indexer.handleRequest(firstEvent, outputStream, CONTEXT));
        final InputStream secondEvent = nextEventYieldsPublishedResources();
        assertDoesNotThrow(() -> indexer.handleRequest(secondEvent, outputStream, CONTEXT));
    }

    @Test
    public void batchIndexerProcessesOneFilePerEvent()
        throws JsonProcessingException {
        indexer.handleRequest(firstEventDoesNotYieldPublishedResources(), outputStream, CONTEXT);
        assertThat(elasticSearchClient.getIndex().keySet(), not(hasItems(in(PUBLISHED_RESOURCES_IDENTIFIERS))));

        indexer.handleRequest(nextEventYieldsPublishedResources(), outputStream, CONTEXT);
        assertThat(elasticSearchClient.getIndex().keySet(), containsInAnyOrder(PUBLISHED_RESOURCES_IDENTIFIERS));
    }

    @Test
    public void batchIndexerReturnsAllIdsForPublishedResourcesThatFailedToBeIndexed()
        throws JsonProcessingException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        indexer = indexerFailingToIndex();

        indexer.handleRequest(firstEventDoesNotYieldPublishedResources(), outputStream, CONTEXT);
        indexer.handleRequest(nextEventYieldsPublishedResources(), outputStream, CONTEXT);

        String outputString = outputStream.toString();
        for (String expectedFailingIdentifier : PUBLISHED_RESOURCES_IDENTIFIERS) {
            assertThat(outputString, containsString(expectedFailingIdentifier));
            assertThat(appender.getMessages(), containsString(expectedFailingIdentifier));
        }
    }

    @Test
    public void batchIndexerLogsAllIdsOfPublishedResourcesThatFailToBeIndexed()
        throws JsonProcessingException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        indexer = indexerFailingToIndex();
        indexer.handleRequest(firstEventDoesNotYieldPublishedResources(), outputStream, CONTEXT);
        indexer.handleRequest(nextEventYieldsPublishedResources(), outputStream, CONTEXT);
        for (String expectedFailingIdentifier : PUBLISHED_RESOURCES_IDENTIFIERS) {
            assertThat(appender.getMessages(), containsString(expectedFailingIdentifier));
        }
    }

    private InputStream nextEventYieldsPublishedResources() throws JsonProcessingException {
        return eventStream(eventBridgeClient.getLatestEvent());
    }

    private InputStream firstEventDoesNotYieldPublishedResources() throws JsonProcessingException {
        return eventStream(initialEvent());
    }

    private ImportDataRequest initialEvent() {
        return new ImportDataRequest("s3://some/location");
    }

    private EventBasedBatchIndexer indexerFailingToIndex() {
        StubElasticSearchHighLevelRestClient failingEsClient = failingElasticSearchClient();
        return new EventBasedBatchIndexer(mockS3Client(), failingEsClient, eventBridgeClient);
    }

    private StubElasticSearchHighLevelRestClient mockEsClient() {
        return new StubElasticSearchHighLevelRestClient();
    }

    private S3Client mockS3Client() {
        return new StubS3Client(RESOURCES);
    }

    private InputStream eventStream(ImportDataRequest eventDetail) throws JsonProcessingException {
        AwsEventBridgeEvent<ImportDataRequest> event = new AwsEventBridgeEvent<>();
        event.setDetail(eventDetail);
        String jsonString = JsonUtils.objectMapperWithEmpty.writeValueAsString(event);
        return IoUtils.stringToStream(jsonString);
    }
}
