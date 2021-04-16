package no.unit.nva.search;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import nva.commons.core.JsonUtils;

public class ImportToSearchIndexHandler implements RequestStreamHandler {


    public ImportToSearchIndexHandler() {
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ImportDataRequest request = JsonUtils.objectMapper.readValue(input, ImportDataRequest.class);

        writeOutput(output, request);
    }

    protected void writeOutput(OutputStream outputStream, ImportDataRequest request)
        throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String outputJson = JsonUtils.objectMapperWithEmpty.writeValueAsString(request);
            writer.write(outputJson);
        }
    }


}
