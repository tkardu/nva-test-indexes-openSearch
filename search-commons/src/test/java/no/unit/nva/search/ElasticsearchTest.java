package no.unit.nva.search;

import static no.unit.nva.search.SearchClient.APPROVED;
import static no.unit.nva.search.SearchClient.ORGANIZATION_IDS;
import static no.unit.nva.search.SearchClient.STATUS;
import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.search.models.EventConsumptionAttributes;
import no.unit.nva.search.models.IndexDocument;
import no.unit.nva.search.models.SearchResourcesResponse;
import no.unit.nva.search.restclients.responses.ViewingScope;
import no.unit.nva.testutils.RandomDataGenerator;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class ElasticsearchTest {

    public static final String ELASTICSEARCH_OSS = "docker.elastic.co/elasticsearch/elasticsearch-oss";
    public static final String INDEX_NAME = RandomDataGenerator.randomString().toLowerCase();
    public static final URI INCLUDED_ORGANIZATION_ID = randomUri();
    public static final URI EXCLUDED_ORGANIZATION_ID = randomUri();
    public static final int ZERO_HITS_BECAUSE_VIEWING_SCOPE_IS_EMPTY = 0;
    public static final int TWO_HITS_BECAUSE_MATCH_ON_BOTH_INCLUDED_UNITS = 2;
    public static final int ONE_HIT_BECAUSE_ONE_UNIT_WAS_EXCLUDED = 1;
    public static final String STATUS_TO_INCLUDE_IN_RESULT = "UNREAD";
    public static final int ZERO_HITS_BECAUSE_APPROVED_WAS_FILTERED_OUT = 0;
    public static final long DELAY_AFTER_INDEXING = 1000L;
    private static final String ELASTICSEARCH_VERSION = "7.10.2";
    private static final int RESULT_SIZE = 10;
    @Container
    public ElasticsearchContainer container = new ElasticsearchContainer(DockerImageName
                                                                             .parse(ELASTICSEARCH_OSS)
                                                                             .withTag(ELASTICSEARCH_VERSION));
    private SearchClient searchClient;
    private IndexingClient indexingClient;

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

        Thread.sleep(DELAY_AFTER_INDEXING);

        SearchResponse response = searchClient.findResourcesForOrganizationIds(getEmptyViewingScope(),
                                                                               RESULT_SIZE,
                                                                               INDEX_NAME);

        assertThat(response.getHits().getHits().length,
                   is(equalTo(ZERO_HITS_BECAUSE_VIEWING_SCOPE_IS_EMPTY)));
    }

    @Test
    void shouldReturnTwoHitsOnViewingScopeWithIncludedUnit() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));

        Thread.sleep(DELAY_AFTER_INDEXING);

        ViewingScope viewingScope = getEmptyViewingScope();
        viewingScope.setIncludedUnits(Set.of(INCLUDED_ORGANIZATION_ID));

        SearchResponse response = searchClient.findResourcesForOrganizationIds(viewingScope,
                                                                               RESULT_SIZE,
                                                                               INDEX_NAME);

        assertThat(response.getHits().getHits().length,
                   is(equalTo(TWO_HITS_BECAUSE_MATCH_ON_BOTH_INCLUDED_UNITS)));
    }

    @Test
    void shouldReturnZeroHitsBecauseStatusIsApproved() throws Exception {
        indexingClient.addDocumentToIndex(
            getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID), APPROVED)
        );

        Thread.sleep(DELAY_AFTER_INDEXING);

        ViewingScope viewingScope = getEmptyViewingScope();
        viewingScope.setIncludedUnits(Set.of(INCLUDED_ORGANIZATION_ID));

        SearchResponse response = searchClient.findResourcesForOrganizationIds(viewingScope,
                                                                               RESULT_SIZE,
                                                                               INDEX_NAME);

        assertThat(response.getHits().getHits().length,
                   is(equalTo(ZERO_HITS_BECAUSE_APPROVED_WAS_FILTERED_OUT)));
    }

    @Test
    void shouldReturnOneHitOnViewingScopeWithExcludedUnit() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID, EXCLUDED_ORGANIZATION_ID)));

        Thread.sleep(DELAY_AFTER_INDEXING);

        var viewingScope = getEmptyViewingScope();
        viewingScope.setIncludedUnits(Set.of(INCLUDED_ORGANIZATION_ID));
        viewingScope.setExcludedUnits(Set.of(EXCLUDED_ORGANIZATION_ID));

        SearchResponse response = searchClient.findResourcesForOrganizationIds(viewingScope,
                                                                               RESULT_SIZE,
                                                                               INDEX_NAME);

        assertThat(response.getHits().getHits().length,
                   is(equalTo(ONE_HIT_BECAUSE_ONE_UNIT_WAS_EXCLUDED)));
    }

    @Test
    void shouldCreateSearchResourcesResponseFromSearchResponse() throws Exception {
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));
        indexingClient.addDocumentToIndex(getIndexDocument(Set.of(INCLUDED_ORGANIZATION_ID)));

        Thread.sleep(DELAY_AFTER_INDEXING);

        var viewingScope = ViewingScope.create(INCLUDED_ORGANIZATION_ID);
        var response = searchClient.findResourcesForOrganizationIds(viewingScope,
                                                                    RESULT_SIZE,
                                                                    INDEX_NAME);
        var searchId = SearchResourcesResponse.createIdWithQuery(randomUri(), null);
        var searchResourcesResponse = SearchResourcesResponse.fromSearchResponse(response, searchId);

        assertThat(searchResourcesResponse, is(notNullValue()));
        assertThat(searchResourcesResponse.getId(), is(equalTo(searchId)));
        assertThat(searchResourcesResponse.getHits().size(), is(equalTo(2)));
    }

    private ViewingScope getEmptyViewingScope() {
        return new ViewingScope();
    }

    private IndexDocument getIndexDocument(Set<URI> organizationIds) {
        return getIndexDocument(organizationIds, STATUS_TO_INCLUDE_IN_RESULT);
    }

    private IndexDocument getIndexDocument(Set<URI> organizationIds, String status) {
        EventConsumptionAttributes eventConsumptionAttributes = new EventConsumptionAttributes(
            INDEX_NAME,
            SortableIdentifier.next()
        );
        Map<String, Object> map = Map.of(
            ORGANIZATION_IDS, organizationIds,
            STATUS, status
        );
        JsonNode jsonNode = objectMapperWithEmpty.convertValue(map, JsonNode.class);
        return new IndexDocument(eventConsumptionAttributes, jsonNode);
    }
}