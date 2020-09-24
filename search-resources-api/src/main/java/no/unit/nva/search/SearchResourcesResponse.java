package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.utils.JacocoGenerated;

import java.util.ArrayList;
import java.util.List;

@JacocoGenerated
@SuppressWarnings("PMD.ShortMethodName")
public class SearchResourcesResponse extends ArrayList<JsonNode> {

    /**
     * Crates a list og strings containing hits from elasticsearch.
     * @param hits from elasticsearch
     * @return SearchResourcesResponse containing hits from ElasticSearch
     */
    public static SearchResourcesResponse of(List<JsonNode> hits) {
        SearchResourcesResponse list = new SearchResourcesResponse();
        list.addAll(hits);
        return list;
    }

}
