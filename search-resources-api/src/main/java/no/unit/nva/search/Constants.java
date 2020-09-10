package no.unit.nva.search;

public class Constants {
    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS_KEY = "ELASTICSEARCH_ENDPOINT_ADDRESS";
    public static final String ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY = "ELASTICSEARCH_ENDPOINT_API_SCHEME";

    // protocol, host , elasicseatch-indexname, searchTerm, size
    public static final String ELASTICSEARCH_SEARCH_ENDPOINT_URI_TEMPLATE = "%s://%s/%s/_search?q=%s&size=%s";

    public Constants() {
    }
}
