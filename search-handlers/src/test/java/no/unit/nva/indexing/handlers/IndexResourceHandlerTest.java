package no.unit.nva.indexing.handlers;

import static no.unit.nva.indexing.handlers.IndexingConfig.objectMapper;
import static no.unit.nva.indexing.handlers.NewIndexDocument.MISSING_IDENTIFIER_IN_RESOURCE;
import static no.unit.nva.indexing.handlers.NewIndexDocument.MISSING_INDEX_NAME_IN_RESOURCE;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.function.Function;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.model.Publication;
import no.unit.nva.publication.PublicationGenerator;
import no.unit.nva.s3.S3Driver;
import no.unit.nva.search.ElasticSearchHighLevelRestClient;
import no.unit.nva.search.RestHighLevelClientWrapper;
import no.unit.nva.stubs.FakeS3Client;
import no.unit.nva.testutils.RandomDataGenerator;
import nva.commons.core.paths.UnixPath;
import nva.commons.core.paths.UriWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class IndexResourceHandlerTest {

    public static final String RESOURCES_INDEX = "resource";
    public static final String SAMPLE_RESOURCE = createSampleResource(Publication::getIdentifier, RESOURCES_INDEX);
    public static final String FILE_DOES_NOT_EXIST = "File does not exist";
    private static final String SAMPLE_RESOURCE_MISSING_IDENTIFIER =
        createSampleResource(ignoredPublication -> null, RESOURCES_INDEX);
    private static final String SAMPLE_RESOURCE_MISSING_INDEX_NAME =
        createSampleResource(Publication::getIdentifier, null);

    private S3Driver s3Driver;
    private RestHighLevelClientWrapper restHighLevelClient;
    private IndexResourceHandler indexResourceHandler;
    private Context context;
    private ByteArrayOutputStream output;

    @BeforeEach
    void init() {
        FakeS3Client fakeS3Client = new FakeS3Client();
        s3Driver = new S3Driver(fakeS3Client, "ignored");

        restHighLevelClient = mock(RestHighLevelClientWrapper.class);
        ElasticSearchHighLevelRestClient searchHighLevelRestClient
            = new ElasticSearchHighLevelRestClient(restHighLevelClient);
        indexResourceHandler = new IndexResourceHandler(s3Driver, searchHighLevelRestClient);

        context = Mockito.mock(Context.class);
        output = new ByteArrayOutputStream();
    }

    @Test
    void shouldAddDocumentToIndexWhenResourceExistsInResourcesStorage() throws Exception {
        URI resourceLocation = prepareEventStorageResourceFile();

        InputStream input = createEventBridgeEvent(resourceLocation);
        indexResourceHandler.handleRequest(input, output, context);

        verify(restHighLevelClient).index(any(), any());
    }

    @Test
    void shouldThrowExceptionOnCommunicationProblemWithService() throws Exception {
        URI resourceLocation = prepareEventStorageResourceFile();

        when(restHighLevelClient.index(any(), any())).thenThrow(IOException.class);

        InputStream input = createEventBridgeEvent(resourceLocation);

        assertThrows(RuntimeException.class,
                     () -> indexResourceHandler.handleRequest(input, output, context));
    }

    @Test
    void shouldThrowExceptionWhenResourceIsMissingIdentifier() throws Exception {
        URI resourceLocation = prepareEventStorageResourceFile(SAMPLE_RESOURCE_MISSING_IDENTIFIER);

        InputStream input = createEventBridgeEvent(resourceLocation);

        RuntimeException exception = assertThrows(RuntimeException.class,
                                                  () -> indexResourceHandler.handleRequest(input, output,
                                                                                           context));

        assertThat(exception.getMessage(), stringContainsInOrder(MISSING_IDENTIFIER_IN_RESOURCE));
    }

    @Test
    void shouldThrowNoSuchKeyExceptionWhenResourceIsMissingFromEventStorage() throws Exception {
        URI missingResourceLocation = RandomDataGenerator.randomUri();

        InputStream input = createEventBridgeEvent(missingResourceLocation);

        NoSuchKeyException exception = assertThrows(NoSuchKeyException.class,
                                                    () -> indexResourceHandler.handleRequest(input, output, context));

        assertThat(exception.getMessage(), stringContainsInOrder(FILE_DOES_NOT_EXIST));
    }

    @Test
    void shouldThrowExceptionWhenResourceIsMissingType() throws Exception {
        URI resourceLocation = prepareEventStorageResourceFile(SAMPLE_RESOURCE_MISSING_INDEX_NAME);

        InputStream input = createEventBridgeEvent(resourceLocation);

        RuntimeException exception = assertThrows(RuntimeException.class,
                                                  () -> indexResourceHandler.handleRequest(input, output,
                                                                                           context));

        assertThat(exception.getMessage(), stringContainsInOrder(MISSING_INDEX_NAME_IN_RESOURCE));
    }

    private static String createSampleResource(Function<Publication, SortableIdentifier> identifierProvider,
                                               String indexName) {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        ObjectNode objectNode = objectMapper.convertValue(publication, ObjectNode.class);
        EventConsumptionAttributes metadata = new EventConsumptionAttributes(indexName,
                                                                             identifierProvider.apply(publication));
        NewIndexDocument newIndexDocument = new NewIndexDocument(metadata, objectNode);
        return attempt(() -> objectMapper.writeValueAsString(newIndexDocument)).orElseThrow();
    }

    private URI prepareEventStorageResourceFile() throws IOException {
        return prepareEventStorageResourceFile(SAMPLE_RESOURCE);
    }

    private URI prepareEventStorageResourceFile(String resource) throws IOException {
        URI resourceLocation = RandomDataGenerator.randomUri();
        UnixPath resourcePath = new UriWrapper(resourceLocation).toS3bucketPath();
        s3Driver.insertFile(resourcePath, resource);
        return resourceLocation;
    }

    private InputStream createEventBridgeEvent(URI resourceLocation) throws JsonProcessingException {
        IndexEvent indexResourceEvent = new IndexEvent();
        indexResourceEvent.setUri(resourceLocation);

        AwsEventBridgeDetail<IndexEvent> detail = new AwsEventBridgeDetail<>();
        detail.setResponsePayload(indexResourceEvent);

        AwsEventBridgeEvent<AwsEventBridgeDetail<IndexEvent>> event = new AwsEventBridgeEvent<>();
        event.setDetail(detail);

        return new ByteArrayInputStream(objectMapperWithEmpty.writeValueAsBytes(event));
    }
}
