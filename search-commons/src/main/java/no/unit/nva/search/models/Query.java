package no.unit.nva.search.models;

import nva.commons.core.paths.UriWrapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class Query {

    private final String searchTerm;
    private final int results;
    private final int from;
    private final String orderBy;
    private final SortOrder sortOrder;
    private final UriWrapper requestUri;

    public Query(String searchTerm, int results, int from, String orderBy, SortOrder sortOrder, UriWrapper requestUri) {
        this.searchTerm = searchTerm;
        this.results = results;
        this.from = from;
        this.orderBy = orderBy;
        this.sortOrder = sortOrder;
        this.requestUri = requestUri;
    }

    public String getSearchTerm() {
        return searchTerm;
    }

    public UriWrapper getRequestUri() {
        return requestUri;
    }

    private SearchSourceBuilder toSearchSourceBuilder() {
        return new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(searchTerm))
                        .sort(orderBy, sortOrder)
                        .from(from)
                        .size(results);
    }

    public SearchRequest toSearchRequest(String index) {
        return new SearchRequest(index).source(toSearchSourceBuilder());
    }
}
