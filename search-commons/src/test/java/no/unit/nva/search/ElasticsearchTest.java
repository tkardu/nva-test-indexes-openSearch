package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.models.EventConsumptionAttributes;
import no.unit.nva.search.models.IndexDocument;
import no.unit.nva.search.restclients.responses.UserResponse;
import no.unit.nva.testutils.RandomDataGenerator;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Testcontainers
public class ElasticsearchTest {

    private final static String ELASTICSEARCH_VERSION = "7.10.2";
    public static final String ELASTICSEARCH_OSS = "docker.elastic.co/elasticsearch/elasticsearch-oss";
    public static final String INDEX_NAME = RandomDataGenerator.randomString().toLowerCase();
    public static final URI INCLUDED_ORGANIZATION_ID = RandomDataGenerator.randomUri();
    public static final URI EXCLUDED_ORGANIZATION_ID = RandomDataGenerator.randomUri();
    public static final int ZERO_HITS_BECAUSE_VIEWING_SCOPE_IS_EMPTY = 0;
    public static final int TWO_HITS_BECAUSE_MATCH_ON_BOTH_INCLUDED_UNITS = 2;
    public static final int ONE_HIT_BECAUSE_ONE_UNIT_WAS_EXCLUDED = 1;


    private SearchClient searchClient;
    private IndexingClient indexingClient;

    @Container
    public ElasticsearchContainer container = new ElasticsearchContainer(DockerImageName
                    .parse(ELASTICSEARCH_OSS)
                    .withTag(ELASTICSEARCH_VERSION));

    @BeforeEach
    void setUp() {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create(container.getHttpHostAddress()));
        RestHighLevelClientWrapper restHighLevelClientWrapper = new RestHighLevelClientWrapper(restClientBuilder);

        searchClient = new SearchClient(restHighLevelClientWrapper);
        indexingClient = new IndexingClient(restHighLevelClientWrapper);
    }

    @Test
    void shouldReturnZeroHitsOnEmptyViewingScope() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of()));

        Thread.sleep(1000L);

        SearchResponse response = searchClient.findResourcesForOrganizationIds(INDEX_NAME, getEmptyViewingScope());

        MatcherAssert.assertThat(response.getHits().getHits().length,
                is(equalTo(ZERO_HITS_BECAUSE_VIEWING_SCOPE_IS_EMPTY)));
    }

    @Test
    void shouldReturnTwoHitsOnViewingScopeWithIncludedUnit() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));

        Thread.sleep(1000L);

        UserResponse.ViewingScope viewingScope = getEmptyViewingScope();
        viewingScope.setIncludedUnits(Set.of(INCLUDED_ORGANIZATION_ID));

        SearchResponse response = searchClient.findResourcesForOrganizationIds(INDEX_NAME, viewingScope);

        MatcherAssert.assertThat(response.getHits().getHits().length,
                is(equalTo(TWO_HITS_BECAUSE_MATCH_ON_BOTH_INCLUDED_UNITS)));
    }

    @Test
    void shouldReturnOneHitOnViewingScopeWithExcludedUnit() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID, EXCLUDED_ORGANIZATION_ID)));

        Thread.sleep(1000L);

        UserResponse.ViewingScope viewingScope = getEmptyViewingScope();
        viewingScope.setIncludedUnits(Set.of(INCLUDED_ORGANIZATION_ID));
        viewingScope.setExcludedUnits(Set.of(EXCLUDED_ORGANIZATION_ID));

        SearchResponse response = searchClient.findResourcesForOrganizationIds(INDEX_NAME, viewingScope);

        MatcherAssert.assertThat(response.getHits().getHits().length,
                is(equalTo(ONE_HIT_BECAUSE_ONE_UNIT_WAS_EXCLUDED)));
    }

    private UserResponse.ViewingScope getEmptyViewingScope() {
        return new UserResponse.ViewingScope();
    }

    private IndexDocument getIndexDocument(Set<URI> organizationIds) {
        EventConsumptionAttributes eventConsumptionAttributes = new EventConsumptionAttributes(
                INDEX_NAME,
                SortableIdentifier.next()
        );
        Map<String, Set<URI>> organizationIdsMap = Map.of(SearchClient.ORGANIZATION_IDS, organizationIds);
        JsonNode jsonNode = objectMapperWithEmpty.convertValue(organizationIdsMap, JsonNode.class);
        return new IndexDocument(eventConsumptionAttributes, jsonNode);
    }

}