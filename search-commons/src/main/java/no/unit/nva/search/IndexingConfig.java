package no.unit.nva.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;

public final class IndexingConfig {

    public static final Environment ENVIRONMENT = new Environment();
    public static final ObjectMapper objectMapper = JsonUtils.dtoObjectMapper;

    private IndexingConfig() {

    }

}
