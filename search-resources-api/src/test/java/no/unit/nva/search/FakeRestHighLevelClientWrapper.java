package no.unit.nva.search;

import java.io.IOException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

public class FakeRestHighLevelClientWrapper extends RestHighLevelClientWrapper{

    private SearchRequest searchRequest;

    public FakeRestHighLevelClientWrapper(RestHighLevelClient client) {
        super(client);
    }

    @Override
    public SearchResponse search(SearchRequest searchRequest, RequestOptions requestOptions) throws IOException {
        this.searchRequest = searchRequest;
        return super.search(searchRequest,requestOptions);
    }

    public SearchRequest getSearchRequest(){
        return this.searchRequest;
    }
}
