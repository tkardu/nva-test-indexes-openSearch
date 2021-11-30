package no.unit.nva.search;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_ADDRESS;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_ENDPOINT_INDEX;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTICSEARCH_REGION;
import static no.unit.nva.search.constants.ApplicationConstants.ELASTIC_SEARCH_SERVICE_NAME;

public final class SearchClientConfig {

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} and index {}";

    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private static final Logger logger = LoggerFactory.getLogger(SearchClientConfig.class);

    private SearchClientConfig() {

    }

    public static SearchClient defaultSearchClient() {
        return new SearchClient(defaultRestHighLevelClientWrapper());
    }

    public static RestHighLevelClientWrapper defaultRestHighLevelClientWrapper() {
        return createElasticsearchClientWithInterceptor(
                ELASTICSEARCH_ENDPOINT_ADDRESS,
                ELASTICSEARCH_ENDPOINT_INDEX
        );
    }

    public static RestHighLevelClientWrapper createElasticsearchClientWithInterceptor(String address, String index) {
        logger.info(INITIAL_LOG_MESSAGE, address, index);

        AWS4Signer signer = getAws4Signer();
        HttpRequestInterceptor interceptor =
                new AWSRequestSigningApacheInterceptor(ELASTIC_SEARCH_SERVICE_NAME,
                        signer,
                        credentialsProvider);

        RestClientBuilder clientBuilder = RestClient
                .builder(HttpHost.create(ELASTICSEARCH_ENDPOINT_ADDRESS))
                .setHttpClientConfigCallback(config -> config.addInterceptorLast(interceptor));
        return new RestHighLevelClientWrapper(clientBuilder);
    }

    private static AWS4Signer getAws4Signer() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(ELASTIC_SEARCH_SERVICE_NAME);
        signer.setRegionName(ELASTICSEARCH_REGION);
        return signer;
    }
}
