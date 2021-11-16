package no.unit.nva.search;

import com.amazonaws.regions.Regions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.s3.S3Client;

public final class BatchIndexingConstants {

    public static final Environment ENVIRONMENT = new Environment();
    public static final String BATCH_INDEX_EVENT_DETAIL_TYPE = "es-batch-index";
    public  static final int NUMBER_OF_FILES_PER_EVENT = 1;
    public static final String AWS_REGION_ENV_VARIABLE = "AWS_REGION";
    private static final Config config = ConfigFactory.load();
    public static final boolean RECURSION_ENABLED = config.getBoolean("batch.index.recursion");
    public static final String BATCH_INDEX_EVENT_BUS_NAME = config.getString("batch.index.eventbusname");

    private BatchIndexingConstants() {
    }

    @JacocoGenerated
    public static EventBridgeClient defaultEventBridgeClient() {
        return EventBridgeClient.builder()
            .httpClient(UrlConnectionHttpClient.create())
            .build();
    }

    @JacocoGenerated
    public static IndexingClient defaultEsClient() {
        return new IndexingClient();
    }

    @JacocoGenerated
    public static S3Client defaultS3Client() {
        String awsRegion = ENVIRONMENT.readEnvOpt(AWS_REGION_ENV_VARIABLE).orElse(Regions.EU_WEST_1.getName());
        return S3Client.builder()
            .region(Region.of(awsRegion))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .build();
    }
}
