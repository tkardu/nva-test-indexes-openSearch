package no.unit.nva.search;

import nva.commons.utils.JacocoGenerated;

import java.util.ArrayList;
import java.util.List;

@JacocoGenerated
@SuppressWarnings("PMD.ShortMethodName")
public class SearchResourcesResponse extends ArrayList<String> {

    public static SearchResourcesResponse of(List<String> hits) {
        SearchResourcesResponse list = new SearchResourcesResponse();
        list.addAll(hits);
        return list;
    }

}
