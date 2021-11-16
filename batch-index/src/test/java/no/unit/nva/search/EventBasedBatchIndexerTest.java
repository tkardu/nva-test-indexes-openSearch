package no.unit.nva.search;

import static no.unit.nva.publication.PublicationGenerator.randomString;
import static no.unit.nva.search.BatchIndexingConstants.NUMBER_OF_FILES_PER_EVENT;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.StringContains.containsString;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.models.EventConsumptionAttributes;
import no.unit.nva.search.models.IndexDocument;
import no.unit.nva.stubs.FakeS3Client;
import nva.commons.core.attempt.Try;
import nva.commons.core.ioutils.IoUtils;
import nva.commons.core.paths.UnixPath;
import nva.commons.core.paths.UriWrapper;
import nva.commons.logutils.LogUtils;
import org.javers.common.collections.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class EventBasedBatchIndexerTest extends BatchIndexTest {

    private EventBasedBatchIndexer indexer;
    private ByteArrayOutputStream outputStream;
    private StubIndexingClient elasticSearchClient;
    private StubEventBridgeClient eventBridgeClient;
    private FakeS3Client s3Client;
    private S3Driver s3Driver;

    @BeforeEach
    public void init() {
        this.outputStream = new ByteArrayOutputStream();
        elasticSearchClient = mockEsClient();
        eventBridgeClient = new StubEventBridgeClient();
        s3Client = new FakeS3Client();
        s3Driver = new S3Driver(s3Client, "ingoredBucket");
        indexer = new EventBasedBatchIndexer(s3Client,
                                             elasticSearchClient,
                                             eventBridgeClient,
                                             NUMBER_OF_FILES_PER_EVENT);
    }

    @Test
    public void batchIndexerParsesEvent() {
        InputStream event = IoUtils.inputStreamFromResources("event.json");
        indexer.handleRequest(event, outputStream, CONTEXT);
    }

    @ParameterizedTest(name = "batch indexer processes n files per request:{0}")
    @ValueSource(ints = {1, 2, 5, 10, 50, 100})
    public void shouldIndexNFilesPerEvent(int numberOfFilesPerEvent) throws IOException {
        indexer = new EventBasedBatchIndexer(s3Client, elasticSearchClient, eventBridgeClient, numberOfFilesPerEvent);
        var expectedFiles = randomFilesInSinceEvent(s3Driver, numberOfFilesPerEvent);
        var unexpectedFile = randomEntryInS3(s3Driver);

        var importLocation = unexpectedFile.getHost().getUri(); //all files are in the same bucket
        InputStream event = eventStream(new ImportDataRequest(importLocation.toString()));
        indexer.handleRequest(event, outputStream, CONTEXT);

        for (var expectedFile : expectedFiles) {
            assertThat(elasticSearchClient.getIndex(), hasKey(expectedFile.getFilename()));
        }
        assertThat(elasticSearchClient.getIndex(), not(hasKey(unexpectedFile.getFilename())));
    }

    @ParameterizedTest(name = "should return all ids for published resources that failed to be indexed. "
                              + "Input size:{0}")
    @ValueSource(ints = {1, 2, 5, 10, 100})
    public void shouldReturnsAllIdsForPublishedResourcesThatFailedToBeIndexed(int numberOfFilesPerEvent)
        throws JsonProcessingException {
        var logger = LogUtils.getTestingAppenderForRootLogger();
        indexer = new EventBasedBatchIndexer(s3Client, failingElasticSearchClient(), eventBridgeClient,
                                             numberOfFilesPerEvent);
        var filesFailingToBeIndexed = randomFilesInSinceEvent(s3Driver, numberOfFilesPerEvent);
        var importLocation = filesFailingToBeIndexed.get(0).getHost().toString();
        var request = new ImportDataRequest(importLocation);
        indexer.handleRequest(eventStream(request), outputStream, CONTEXT);
        var actualIdentifiersOfNonIndexedEntries =
            Arrays.asList(IndexingConfig.objectMapper.readValue(outputStream.toString(), String[].class));
        var expectedIdentifiesOfNonIndexedEntries = extractIdentifiersFromFailingFiles(filesFailingToBeIndexed);

        assertThat(actualIdentifiersOfNonIndexedEntries, containsInAnyOrder(expectedIdentifiesOfNonIndexedEntries));

        for (var expectedIdentifier : expectedIdentifiesOfNonIndexedEntries) {
            assertThat(logger.getMessages(), containsString(expectedIdentifier));
        }
    }

    @Test
    void shouldEmitEventForProcessingNextBatchWhenThereAreMoreFilesToProcess() throws IOException {
        var firstFile = randomEntryInS3(s3Driver);
        randomEntryInS3(s3Driver); // necessary second file for the emission of the next event

        String bucketUri = firstFile.getHost().getUri().toString();
        ImportDataRequest firstEvent = new ImportDataRequest(bucketUri);
        var event = eventStream(firstEvent);

        indexer.handleRequest(event, outputStream, CONTEXT);
        assertThat(eventBridgeClient.getLatestEvent().getStartMarker(), is(equalTo(firstFile.getFilename())));
    }

    @Test
    void shouldNotEmitEventWhenThereAreNoMoreFilesToProcess() throws IOException {
        var firstFile = randomEntryInS3(s3Driver);
        var secondFile = randomEntryInS3(s3Driver);

        String bucketUri = firstFile.getHost().getUri().toString();
        ImportDataRequest lastEvent = new ImportDataRequest(bucketUri, firstFile.getFilename());
        var event = eventStream(lastEvent);

        indexer.handleRequest(event, outputStream, CONTEXT);
        assertThat(eventBridgeClient.getLatestEvent(), is(nullValue()));
    }

    @Test
    void shouldIndexFirstFilesInFirstEventAndSubsequentFilesInNextEvent() throws IOException {
        var firstFile = randomEntryInS3(s3Driver);
        var secondFile = randomEntryInS3(s3Driver);
        String bucketUri = firstFile.getHost().getUri().toString();

        ImportDataRequest firstEvent = new ImportDataRequest(bucketUri);
        indexer.handleRequest(eventStream(firstEvent), outputStream, CONTEXT);

        assertThat(elasticSearchClient.getIndex(), hasKey(firstFile.getFilename()));
        assertThat(elasticSearchClient.getIndex(), not(hasKey(secondFile.getFilename())));

        ImportDataRequest secondEvent = new ImportDataRequest(bucketUri, firstFile.getFilename());
        indexer.handleRequest(eventStream(secondEvent), outputStream, CONTEXT);

        assertThat(elasticSearchClient.getIndex(), hasKey(firstFile.getFilename()));
        assertThat(elasticSearchClient.getIndex(), hasKey(secondFile.getFilename()));
    }

    private String[] extractIdentifiersFromFailingFiles(List<UriWrapper> filesFailingToBeIndexed) {
        return filesFailingToBeIndexed
            .stream()
            .map(UriWrapper::getFilename)
            .collect(Collectors.toList())
            .toArray(String[]::new);
    }

    private List<UriWrapper> randomFilesInSinceEvent(S3Driver s3Driver, int numberOfFilesPerEvent) {
        return IntStream.range(0, numberOfFilesPerEvent)
            .boxed()
            .map(attempt(ignored -> randomEntryInS3(s3Driver)))
            .map(Try::orElseThrow)
            .collect(Collectors.toList());
    }

    private UriWrapper randomEntryInS3(S3Driver s3Driver) throws IOException {
        var randomIndexDocument = randomIndexDocument();
        var filePath = UnixPath.of(randomIndexDocument.getDocumentIdentifier());
        return new UriWrapper(s3Driver.insertFile(filePath, randomIndexDocument.toJsonString()));
    }

    private IndexDocument randomIndexDocument() {
        return new IndexDocument(randomEventConsumptionAttributes(), randomObject());
    }

    private JsonNode randomObject() {
        var json = IndexingConfig.objectMapper.createObjectNode();
        json.put(randomString(), randomString());
        return json;
    }

    private EventConsumptionAttributes randomEventConsumptionAttributes() {
        return new EventConsumptionAttributes(randomString(), SortableIdentifier.next());
    }

    private StubIndexingClient mockEsClient() {
        return new StubIndexingClient();
    }

    private InputStream eventStream(ImportDataRequest eventDetail) throws JsonProcessingException {
        AwsEventBridgeEvent<ImportDataRequest> event = new AwsEventBridgeEvent<>();
        event.setDetail(eventDetail);
        String jsonString = objectMapperWithEmpty.writeValueAsString(event);
        return IoUtils.stringToStream(jsonString);
    }
}
