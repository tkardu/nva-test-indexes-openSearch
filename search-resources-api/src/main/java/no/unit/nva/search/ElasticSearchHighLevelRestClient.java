package no.unit.nva.search;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.search.exception.SearchException;
import nva.commons.exceptions.ApiGatewayException;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticSearchHighLevelRestClient {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchHighLevelRestClient.class);

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";

    private static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String SOURCE_JSON_POINTER = "/_source";
    public static final String HITS_JSON_POINTER = "/hits/hits";

    private final String elasticSearchEndpointAddress;


    private static final String serviceName = "es";
    private static final String region = "eu-west-1";
//    private static final String elasticSearchEndpoint = "https://search-sg-elas-nvaela-l01tk4es8umt-26a2ccz2exi6xkxqg46y7ry63q.eu-west-1.es.amazonaws.com"; // e.g. https://search-mydomain.us-west-1.es.amazonaws.com

    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private final String elasticSearchEndpointIndex;


    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param environment Environment with properties
     */
    public ElasticSearchHighLevelRestClient(Environment environment) {
        elasticSearchEndpointAddress = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        String elasticSearchEndpointScheme = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);

        logger.info(INITIAL_LOG_MESSAGE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Searches for an term or index:term in elasticsearch index.
     * @param term search argument
     * @param results number of results
     * @throws ApiGatewayException thrown when uri is misconfigured, service i not available or interrupted
     */
    public SearchResourcesResponse searchSingleTerm(String term, String results) throws ApiGatewayException {

        try (RestHighLevelClient esClient =
                     createElasticsearchClient(serviceName, region, elasticSearchEndpointAddress) ) {
            SimpleQueryStringBuilder builder = QueryBuilders.simpleQueryStringQuery(term);

            final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(builder);
            final SearchRequest searchRequest = new SearchRequest(elasticSearchEndpointIndex);

            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchResponse.toString());
            System.out.println("results="+results);
            SearchResourcesResponse searchResourcesResponse = toSearchResourcesResponse(searchResponse.toString());
            return searchResourcesResponse;

        } catch (IOException e) {
            throw new SearchException(e.getMessage(), e);
        }
    }

    private SearchResourcesResponse toSearchResourcesResponse(String body) throws JsonProcessingException {
        JsonNode values = mapper.readTree(body);
        List<JsonNode> sourceList = extractSourceList(values);
        return SearchResourcesResponse.of(sourceList);
    }


    private List<JsonNode> extractSourceList(JsonNode record) {
        return toStream(record.at(HITS_JSON_POINTER))
                .map(this::extractSourceStripped)
                .collect(Collectors.toList());
    }

    @JacocoGenerated
    private JsonNode extractSourceStripped(JsonNode record) {
        JsonNode jsonNode = record.at(SOURCE_JSON_POINTER);
        return jsonNode;
    }


    private Stream<JsonNode> toStream(JsonNode node) {
        return StreamSupport.stream(node.spliterator(), false);
    }



    // Adds the interceptor to the ES REST client
    private static RestHighLevelClient createElasticsearchClient(String serviceName, String region, String elasticSearchEndpoint) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(elasticSearchEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }

}
