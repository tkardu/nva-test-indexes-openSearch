package no.unit.nva.search;

import nva.commons.utils.JacocoGenerated;

import java.util.ArrayList;
import java.util.List;

@JacocoGenerated
@SuppressWarnings("PMD.ShortMethodName")
public class SearchResourcesResponse extends ArrayList<String> {

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
