package no.unit.nva.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.commons.json.JsonUtils;
import nva.commons.core.Environment;

public final class IndexingConfig {

    public static final Environment ENVIRONMENT = new Environment();
    public static final ObjectMapper objectMapper = JsonUtils.dtoObjectMapper;

    private IndexingConfig() {

    }

}
