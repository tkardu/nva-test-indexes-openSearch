package no.unit.nva.search;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class ImportToSearchIndexHandler implements RequestStreamHandler {

    public static final String AWS_REGION_ENV_VARIABLE = "AWS_REGION";
    private static final Logger logger = LoggerFactory.getLogger(ImportToSearchIndexHandler.class);
    private final ElasticSearchHighLevelRestClient elasticSearchRestClient;
    private final S3Client s3Client;

    @JacocoGenerated
    public ImportToSearchIndexHandler() {
        this(new Environment());
    }

    @JacocoGenerated
    public ImportToSearchIndexHandler(Environment environment) {
        this(defaultS3Client(environment), defaultEsClient());
    }

    public ImportToSearchIndexHandler(S3Client s3Client,
                                      ElasticSearchHighLevelRestClient elasticSearchRestClient) {
        this.s3Client = s3Client;
        this.elasticSearchRestClient = elasticSearchRestClient;
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ImportDataRequest request = parseInput(input);
        List<String> failures = new BatchIndexer(request, s3Client, elasticSearchRestClient).processRequest();
        writeOutput(output, failures);
    }



    protected void writeOutput(OutputStream outputStream, List<String> failures)
        throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String outputJson = JsonUtils.objectMapperWithEmpty.writeValueAsString(failures);
            writer.write(outputJson);
        }
    }

    @JacocoGenerated
    private static ElasticSearchHighLevelRestClient defaultEsClient() {
        return new ElasticSearchHighLevelRestClient();
    }

    @JacocoGenerated
    private static S3Client defaultS3Client(Environment environment) {
        String awsRegion = environment.readEnvOpt(AWS_REGION_ENV_VARIABLE).orElse(Regions.EU_WEST_1.getName());
        return S3Client.builder()
            .region(Region.of(awsRegion))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .build();
    }

    private ImportDataRequest parseInput(InputStream input) throws IOException {
        ImportDataRequest request = JsonUtils.objectMapper.readValue(input, ImportDataRequest.class);
        logger.info("Bucket: " + request.getBucket());
        logger.info("Path: " + request.getS3Path());
        return request;
    }
}
