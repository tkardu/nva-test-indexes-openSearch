package no.unit.nva.search.constants;

import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;

public final class ApplicationConstants {

    public static final String ELASTIC_SEARCH_SERVICE_NAME = "es";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";

    public static final Environment ENVIRONMENT = new Environment();
    public static final String ELASTICSEARCH_REGION = readElasticSearchRegion();
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = readElasticSearchEndpointAddress();

    public static final ObjectMapper objectMapperWithEmpty = JsonUtils.dtoObjectMapper;
    public static final ObjectMapper objectMapperNoEmpty = JsonUtils.dynamoObjectMapper;


    private ApplicationConstants() {

    }

    private static String readElasticSearchRegion() {
        return ENVIRONMENT.readEnv("ELASTICSEARCH_REGION");
    }

    private static String readElasticSearchEndpointAddress() {
        return ENVIRONMENT.readEnv("ELASTICSEARCH_ENDPOINT_ADDRESS");
    }
}
