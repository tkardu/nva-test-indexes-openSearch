package no.unit.nva.search;

public class Constants {
    public static final String CREATED_DATE_KEY = "createdDate";
    public static final String TARGET_SERVICE_URL_KEY = "TARGET_SERVICE_URL";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String IDENTIFIER = "identifier";
    public static final String OWNER_NAME_KEY = "owner";
    public static final String MODIFIED_DATE_KEY = "modifiedDate";
    public static final String DATE_YEAR = "entityDescription.date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTITY_NAME = "entityDescription.contributors.identity.name";
    public static final String PUBLICATION_TYPE = "type";
    public static final String SIMPLE_DOT_SEPARATOR = ".";
    public static final String EMPTY_STRING = "";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS_KEY = "ELASTICSEARCH_ENDPOINT_ADDRESS";
    public static final String ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY = "ELASTICSEARCH_ENDPOINT_API_SCHEME";
    public static final String ELASTICSEARCH_DOCUMENT_OPERATION = "_doc";
    public static final String ELASTICSEARCH_SEARCH_ENDPOINT = "_search";


    // protocol, host , elasicseatch-indexname, searchTerm
    public static final String ELASTICSEARCH_SEARCH_ENDPOINT_URI_TEMPLATE = "%s://%s/%s/_search?q=%s";

    public Constants() {
    }
}
