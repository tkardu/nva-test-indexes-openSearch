package no.unit.nva.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JacocoGenerated
public class FramedJsonGenerator {

    public static final ObjectMapper mapper = JsonUtils.objectMapper;
    public static final String JSON_LD_GRAPH = "@graph";
    private final Map<String, Object> framedJson;

    public FramedJsonGenerator(List<InputStream> streams, InputStream frame) {
        Map<?, ?> frameMap = null;
        try {
            frameMap = mapper.readValue(frame, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        framedJson = JsonLdProcessor.frame(createGraphDocumentFromInputStreams(streams),
                Objects.requireNonNull(frameMap), getDefaultOptions());
    }

    @SuppressWarnings("PMD.CloseResource")
    @JacocoGenerated
    private Map<String, Object> createGraphDocumentFromInputStreams(List<InputStream> streams) {
        ObjectNode doc = mapper.createObjectNode();
        ArrayNode graph = mapper.createArrayNode();
        for (InputStream stream : streams) {
            try {
                JsonNode node = mapper.readTree(stream);
                graph.add(node);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        doc.set(JSON_LD_GRAPH, graph);
        return mapper.convertValue(doc, new TypeReference<>() { });
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
