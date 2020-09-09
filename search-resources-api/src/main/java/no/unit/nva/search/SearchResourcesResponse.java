package no.unit.nva.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;

@JacocoGenerated
@SuppressWarnings("PMD.ShortMethodName")
public class SearchResourcesResponse extends ArrayList<String> {

    private static final ObjectMapper mapper = JsonUtils.objectMapper;

    /**
     * Crates a list og strings containing hits from elasticsearch.
     * @param hits from elasticsearch
     * @return
     */
    public static SearchResourcesResponse of(List<String> hits) {
        SearchResourcesResponse list = new SearchResourcesResponse();
        list.addAll(hits);
        return list;
    }

}
