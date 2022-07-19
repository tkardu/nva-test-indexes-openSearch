package no.unit.nva.search.constants;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import no.unit.nva.commons.json.JsonUtils;
import nva.commons.core.Environment;

public final class ApplicationConstants {
    
    public static final String ELASTIC_SEARCH_SERVICE_NAME = "es";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    public static final String RESOURCES_INDEX = "resources";
    public static final String DOIREQUESTS_INDEX = "doirequests";
    public static final String MESSAGES_INDEX = "messages";
    public static final String PUBLISHING_REQUESTS_INDEX = "publishingrequests";
    
    public static final List<String> TICKET_INDICES =
        List.of(DOIREQUESTS_INDEX, MESSAGES_INDEX, PUBLISHING_REQUESTS_INDEX);
    
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
