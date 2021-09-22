package no.unit.nva.search;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import nva.commons.core.ioutils.IoUtils;
import nva.commons.logutils.LogUtils;
import nva.commons.logutils.TestAppender;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImportToSearchIndexHandlerTest extends BatchIndexTest {

    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    private static final String SOME_S3_LOCATION = "s3://some-bucket/some/path";

    private StubS3Client s3Client;
    private String importRequest;
    private ByteArrayOutputStream outputStream;
    private StubElasticSearchHighLevelRestClient mockElasticSearchClient;

    @BeforeEach
    public void initialize() {
        s3Client = new StubS3Client(RESOURCES);
        importRequest = new ImportDataRequest(SOME_S3_LOCATION).toJsonString();
        outputStream = new ByteArrayOutputStream();

        mockElasticSearchClient = new StubElasticSearchHighLevelRestClient();
    }

    @Test
    public void handlerIndexesAllPublicationsStoredInResourceFiles() throws IOException {
        s3Client = new StubS3Client(RESOURCES);
        ImportToSearchIndexHandler handler = newHandler();

        handler.handleRequest(newImportRequest(), outputStream, CONTEXT);
        var actualIdentifiers = mockElasticSearchClient.getIndex().keySet();

        Set<String> expectedIdentifiers = new HashSet<>(Arrays.asList(PUBLISHED_RESOURCES_IDENTIFIERS));
        Diff diff = JaversBuilder.javers()
            .build()
            .compare(expectedIdentifiers, actualIdentifiers);
        assertThat(diff.prettyPrint(), actualIdentifiers, is(equalTo(expectedIdentifiers)));
    }

    @Test
    public void handlerReturnsErrorEntryForEveryFailedIndexAction() throws IOException {
        String outputString = handlerFailsToInsertPublications();
        for (String failedImportIdentifier : PUBLISHED_RESOURCES_IDENTIFIERS) {
            assertThat(outputString, containsString(failedImportIdentifier));
        }
        assertThat(outputString, containsString(EXPECTED_EXCEPTION_MESSAGE));
    }

    @Test
    public void handlerLogsErrorEntryForEveryFailedIndexAction() throws IOException {
        TestAppender appender = LogUtils.getTestingAppenderForRootLogger();
        handlerFailsToInsertPublications();
        for (String failedImportIdentifier : PUBLISHED_RESOURCES_IDENTIFIERS) {
            assertThat(appender.getMessages(), containsString(failedImportIdentifier));
        }

        assertThat(appender.getMessages(), containsString(EXPECTED_EXCEPTION_MESSAGE));
    }

    protected String handlerFailsToInsertPublications() throws IOException {
        s3Client = new StubS3Client(RESOURCES);
        mockElasticSearchClient = failingElasticSearchClient();
        ImportToSearchIndexHandler handler = new ImportToSearchIndexHandler(s3Client, mockElasticSearchClient);
        handler.handleRequest(newImportRequest(), outputStream, CONTEXT);
        return outputStream.toString();
    }

    private ImportToSearchIndexHandler newHandler() {
        return new ImportToSearchIndexHandler(s3Client, mockElasticSearchClient);
    }


    private InputStream newImportRequest() {
        return IoUtils.stringToStream(importRequest);
    }
}
