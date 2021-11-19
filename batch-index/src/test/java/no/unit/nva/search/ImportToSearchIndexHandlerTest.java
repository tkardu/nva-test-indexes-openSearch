package no.unit.nva.search;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import nva.commons.core.ioutils.IoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImportToSearchIndexHandlerTest extends BatchIndexTest {

    private static final String SOME_S3_LOCATION = "s3://some-bucket/some/path";

    private ImportDataRequestEvent importRequest;
    private ByteArrayOutputStream outputStream;

    private StubEventBridgeClient eventBridgeClient;

    @BeforeEach
    public void initialize() {

        importRequest = new ImportDataRequestEvent(SOME_S3_LOCATION);
        outputStream = new ByteArrayOutputStream();
        eventBridgeClient = new StubEventBridgeClient();
    }

    @Test
    public void handlerSendsEventToEventBridgeWhenItReceivesAnImportRequest() throws IOException {

        ImportToSearchIndexHandler handler = newHandler();

        handler.handleRequest(newImportRequest(), outputStream, CONTEXT);
        assertThat(eventBridgeClient.getLatestEvent(), is(equalTo(importRequest)));
    }

    private ImportToSearchIndexHandler newHandler() {
        return new ImportToSearchIndexHandler(eventBridgeClient);
    }

    private InputStream newImportRequest() {
        return IoUtils.stringToStream(importRequest.toJsonString());
    }
}
