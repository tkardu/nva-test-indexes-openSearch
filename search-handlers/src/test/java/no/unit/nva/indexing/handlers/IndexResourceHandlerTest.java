package no.unit.nva.indexing.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import no.unit.nva.events.models.AwsEventBridgeDetail;
import no.unit.nva.events.models.AwsEventBridgeEvent;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static no.unit.nva.indexing.handlers.IndexResourceWrapper.MISSING_IDENTIFIER_IN_RESOURCE;
import static no.unit.nva.indexing.handlers.IndexResourceWrapper.MISSING_INDEX_NAME_IN_RESOURCE;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexResourceHandlerTest {

    public static final String SIMPLE_RESOURCE = "{ \"type\": \"Resource\", \"identifier\": \"123\" }";
    public static final String SIMPLE_RESOURCE_MISSING_TYPE = "{ \"type\": null, \"identifier\": \"123\" }";
    public static final String SIMPLE_RESOURCE_MISSING_IDENTIFIER = "{ \"type\": \"Resource\", \"identifier\": null }";
    public static final String FILE_DOES_NOT_EXIST = "File does not exist";

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
    void shouldAddDocumentToIndexWhenResourceExistsInEventStorage() throws Exception {
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
        URI resourceLocation = prepareEventStorageResourceFile(SIMPLE_RESOURCE_MISSING_IDENTIFIER);

        InputStream input = createEventBridgeEvent(resourceLocation);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> indexResourceHandler.handleRequest(input, output, context));

        assertThat(exception.getMessage(), stringContainsInOrder(MISSING_IDENTIFIER_IN_RESOURCE));
    }

    @Test
    void shouldThrowExceptionWhenResourceIsMissingType() throws Exception {
        URI resourceLocation = prepareEventStorageResourceFile(SIMPLE_RESOURCE_MISSING_TYPE);

        InputStream input = createEventBridgeEvent(resourceLocation);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> indexResourceHandler.handleRequest(input, output, context));

        assertThat(exception.getMessage(), stringContainsInOrder(MISSING_INDEX_NAME_IN_RESOURCE));

    }

    @Test
    void shouldThrowNoSuchKeyExceptionWhenResourceIsMissingFromEventStorage() throws Exception {
        URI missingResourceLocation = RandomDataGenerator.randomUri();

        InputStream input = createEventBridgeEvent(missingResourceLocation);

        NoSuchKeyException exception = assertThrows(NoSuchKeyException.class,
                () -> indexResourceHandler.handleRequest(input, output, context));

        assertThat(exception.getMessage(), stringContainsInOrder(FILE_DOES_NOT_EXIST));

    }

    private URI prepareEventStorageResourceFile() {
        return prepareEventStorageResourceFile(SIMPLE_RESOURCE);
    }

    private URI prepareEventStorageResourceFile(String resource) {
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
