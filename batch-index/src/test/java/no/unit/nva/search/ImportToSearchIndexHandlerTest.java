package no.unit.nva.search;

import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY;
import static no.unit.nva.search.ElasticSearchHighLevelRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import no.unit.nva.search.exception.SearchException;
import nva.commons.core.Environment;
import nva.commons.core.ioutils.IoUtils;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImportToSearchIndexHandlerTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    public static final String INPUT_1 = "input1.ion.gz";
    public static final String INPUT_2 = "input2.ion.gz";
    public static final String INPUT_3 = "input3.ion.gz";
    public static final String INPUT_4 = "input4.ion.gz";
    public static final String[] RESOURCES = {INPUT_1, INPUT_2, INPUT_3, INPUT_4};
    public static final Context CONTEXT = mock(Context.class);
    public static final String PUBLISHED_RESOURCES_IDENTIFIERS = "published_resources_identifiers_for_all_files.txt";
    public static final String EXPECTED_EXCEPTION_MESSAGE = "expectedMessage";
    private static final String SOME_S3_LOCATION = "s3://some-bucket/some/path";

    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";

    private Environment mockEnvironment = setupMockEnvironment();
    private StubS3Client s3Client;
    private String importRequest;
    private ByteArrayOutputStream outputStream;
    private StubElasticSearchHighLevelRestClient mockElasticSearchClient;

    @BeforeEach
    public void initialize() {
        s3Client = new StubS3Client(RESOURCES);
        importRequest = new ImportDataRequest(SOME_S3_LOCATION).toJsonString();
        outputStream = new ByteArrayOutputStream();
        mockEnvironment = setupMockEnvironment();
        mockElasticSearchClient = new StubElasticSearchHighLevelRestClient(mockEnvironment);
    }

    @Test
    public void handlerIndexesAllPublicationsStoredInResourceFiles() throws IOException {
        s3Client = new StubS3Client(RESOURCES);
        ImportToSearchIndexHandler handler = newHandler();

        handler.handleRequest(newImportRequest(), outputStream, CONTEXT);
        var actualIdentifiers = mockElasticSearchClient.getIndex().keySet();

        Set<String> expectedIdentifiers = constructExpectedListOfPublicationIdentifiers();
        Diff diff = JaversBuilder.javers()
                        .build()
                        .compare(expectedIdentifiers, actualIdentifiers);
        assertThat(diff.prettyPrint(), actualIdentifiers, is(equalTo(expectedIdentifiers)));
    }

    @Test
    public void handlerReturnsErrorEntryForEveryFailedIndexAction() throws IOException {
        String outputString = handlerFailsToInsertPublications();

        Set<String> expectedIdentifiers = constructExpectedListOfPublicationIdentifiers();
        for (String failedImportIdentifier : expectedIdentifiers) {
            assertThat(outputString, containsString(failedImportIdentifier));
        }
        assertThat(outputString, containsString(EXPECTED_EXCEPTION_MESSAGE));
    }

    @Test
    public void handlerLogsErrorEntryForEveryFailedIndexAction() throws IOException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        handlerFailsToInsertPublications();
        Set<String> expectedIdentifiers = constructExpectedListOfPublicationIdentifiers();
        for (String failedImportIdentifier : expectedIdentifiers) {
            assertThat(appender.getMessages(), containsString(failedImportIdentifier));
        }

        assertThat(appender.getMessages(), containsString(EXPECTED_EXCEPTION_MESSAGE));
    }

    private String handlerFailsToInsertPublications() throws IOException {
        s3Client = new StubS3Client(RESOURCES);
        mockElasticSearchClient = failingElasticSearch();
        ImportToSearchIndexHandler handler = newHandler();
        handler.handleRequest(newImportRequest(), outputStream, CONTEXT);
        return outputStream.toString();
    }

    private StubElasticSearchHighLevelRestClient failingElasticSearch() {
        return new StubElasticSearchHighLevelRestClient(mockEnvironment) {
            @Override
            public void addDocumentToIndex(IndexDocument document) throws SearchException {
                throw new SearchException(document.getId().toString(),
                                          new RuntimeException(EXPECTED_EXCEPTION_MESSAGE));
            }
        };
    }

    private ImportToSearchIndexHandler newHandler() {
        return new ImportToSearchIndexHandler(s3Client, mockElasticSearchClient);
    }

    private Environment setupMockEnvironment() {
        Environment environment = mock(Environment.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
            .readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        return environment;
    }

    private Set<String> constructExpectedListOfPublicationIdentifiers() {
        return new HashSet<>(IoUtils.linesfromResource(Path.of(PUBLISHED_RESOURCES_IDENTIFIERS)));
    }

    private InputStream newImportRequest() {
        return IoUtils.stringToStream(importRequest);
    }
}