package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import nva.commons.core.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import static no.unit.nva.search.BatchIndexingConstants.defaultEventBridgeClient;
import static no.unit.nva.search.EmitEventUtils.emitEvent;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;


public class ImportToSearchIndexHandler implements RequestStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(ImportToSearchIndexHandler.class);
    private final EventBridgeClient eventBridgeClient;

    @JacocoGenerated
    public ImportToSearchIndexHandler() {
        this(defaultEventBridgeClient());
    }

    public ImportToSearchIndexHandler(EventBridgeClient eventBridgeClient) {
        this.eventBridgeClient = eventBridgeClient;
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ImportDataRequest request = parseInput(input);
        emitEvent(eventBridgeClient, request, context);
        writeOutput(output);
    }

    protected void writeOutput(OutputStream outputStream)
        throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String outputJson = objectMapperWithEmpty.writeValueAsString("OK");
            writer.write(outputJson);
        }
    }

    private ImportDataRequest parseInput(InputStream input) throws IOException {
        ImportDataRequest request = objectMapperWithEmpty.readValue(input, ImportDataRequest.class);
        logger.info("Bucket: " + request.getBucket());
        logger.info("Path: " + request.getS3Path());
        return request;
    }
}
