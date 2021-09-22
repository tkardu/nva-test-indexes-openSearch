package no.unit.nva.search;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.testutils.IoUtils;
import nva.commons.core.JsonUtils;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

public class EventBasedBatchIndexerTest extends BatchIndexTest {

    EventBasedBatchIndexer indexer;
    private ByteArrayOutputStream outputStream;
    private StubElasticSearchHighLevelRestClient elasticSearchClient;

    @BeforeEach
    public void init() {
        this.outputStream = new ByteArrayOutputStream();
        elasticSearchClient = mockEsClient();
        indexer = new EventBasedBatchIndexer(mockS3Client(), elasticSearchClient);
    }

    @Test
    public void batchIndexerThrowsNoExceptionWhenValidInputIsSupplied() throws JsonProcessingException {
        InputStream inputStream = eventStream();
        assertDoesNotThrow(() -> indexer.handleRequest(inputStream, outputStream, CONTEXT));
    }

    @Test
    public void batchIndexerIndexesAllPublishedPublicationsWhenInputWithPublishedPublicationsIsSupplied()
        throws JsonProcessingException {
        InputStream inputStream = eventStream();
        indexer.handleRequest(inputStream, outputStream, CONTEXT);
        assertThat(elasticSearchClient.getIndex().keySet(), containsInAnyOrder(PUBLISHED_RESOURCES_IDENTIFIERS));
    }

    @Test
    public void batchIndexerReturnsAllIdsForPublishedResourcesThatFailedToBeIndexed()
        throws JsonProcessingException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        InputStream inputStream = eventStream();
        indexer = indexerFailingToIndex();
        indexer.handleRequest(inputStream, outputStream, CONTEXT);
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
        InputStream inputStream = eventStream();
        indexer = indexerFailingToIndex();
        indexer.handleRequest(inputStream, outputStream, CONTEXT);
        for (String expectedFailingIdentifier : PUBLISHED_RESOURCES_IDENTIFIERS) {
            assertThat(appender.getMessages(), containsString(expectedFailingIdentifier));
        }
    }

    private EventBasedBatchIndexer indexerFailingToIndex() {
        StubElasticSearchHighLevelRestClient failingEsClient = failingElasticSearchClient();
        return new EventBasedBatchIndexer(mockS3Client(), failingEsClient);
    }

    private StubElasticSearchHighLevelRestClient mockEsClient() {
        return new StubElasticSearchHighLevelRestClient();
    }

    private S3Client mockS3Client() {
        return new StubS3Client(RESOURCES);
    }

    private InputStream eventStream() throws JsonProcessingException {
        ImportDataRequest dataRequest = new ImportDataRequest("s3://some/location");
        AwsEventBridgeEvent<ImportDataRequest> event = new AwsEventBridgeEvent<>();
        event.setDetail(dataRequest);
        String jsonString = JsonUtils.objectMapperWithEmpty.writeValueAsString(event);
        return IoUtils.stringToStream(jsonString);
    }
}