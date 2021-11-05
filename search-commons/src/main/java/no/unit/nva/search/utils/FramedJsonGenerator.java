package no.unit.nva.search.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.attempt.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

@JacocoGenerated
public class FramedJsonGenerator {

    public static final String JSON_LD_GRAPH = "@graph";
    private static final Logger logger = LoggerFactory.getLogger(FramedJsonGenerator.class);
    private final Map<String, Object> framedJson;

    public FramedJsonGenerator(List<InputStream> streams, InputStream frame) {
        framedJson = attempt(() -> objectMapperWithEmpty.readValue(frame, Map.class))
                .toOptional(fail -> logFramingFailure(fail.getException()))
                .map(map -> createFramedJson(streams, map))
                .orElseThrow();
    }

    private Map<String, Object> createFramedJson(List<InputStream> streams, Map<?, ?> frameMap) {
        return JsonLdProcessor.frame(createGraphDocumentFromInputStreams(streams),
                Objects.requireNonNull(frameMap), getDefaultOptions());
    }

    private Map<String, Object> createGraphDocumentFromInputStreams(List<InputStream> streams) {
        ObjectNode document = objectMapperWithEmpty.createObjectNode();
        ArrayNode graph = objectMapperWithEmpty.createArrayNode();
        streams.stream()
                .map(attempt(objectMapperWithEmpty::readTree))
                .filter(this::keepSuccessesAndLogErrors)
                .map(Try::orElseThrow)
                .forEach(graph::add);

        document.set(JSON_LD_GRAPH, graph);
        return objectMapperWithEmpty.convertValue(document, new TypeReference<>() {
        });
    }

    private boolean keepSuccessesAndLogErrors(Try<JsonNode> jsonNodeTry) {
        if (jsonNodeTry.isFailure()) {
            logFramingFailure(jsonNodeTry.getException());
        }
        return jsonNodeTry.isSuccess();
    }

    private void logFramingFailure(Exception exception) {
        logger.warn("Framing failed:", exception);
    }

    public String getFramedJson() throws IOException {
        return com.github.jsonldjava.utils.JsonUtils.toPrettyString(framedJson);
    }

    private JsonLdOptions getDefaultOptions() {
        JsonLdOptions options = new JsonLdOptions();
        options.setOmitGraph(true);
        options.setPruneBlankNodeIdentifiers(true);
        return options;
    }
}
