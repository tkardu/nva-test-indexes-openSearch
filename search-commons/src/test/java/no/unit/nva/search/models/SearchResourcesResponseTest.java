package no.unit.nva.search.models;

import static com.spotify.hamcrest.jackson.JsonMatchers.jsonObject;
import static no.unit.nva.commons.json.JsonUtils.dtoObjectMapper;
import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValues;
import static no.unit.nva.testutils.RandomDataGenerator.randomInteger;
import static no.unit.nva.testutils.RandomDataGenerator.randomJson;
import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.spotify.hamcrest.jackson.JsonMatchers;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import nva.commons.core.attempt.Try;
import org.junit.jupiter.api.Test;

class SearchResourcesResponseTest {

    @Test
    void builderReturnsObjectWithoutAnyEmptyField() {
        SearchResourcesResponse response = randomResponse();
        assertThat(response, doesNotHaveEmptyValues());
    }

    @Test
    void jsonSerializationShouldKeepDeprecatedFieldsUntilFrontendHasMigrated() throws JsonProcessingException {
        SearchResourcesResponse searchResponse = randomResponse();
        var serialized = dtoObjectMapper.writeValueAsString(searchResponse);
        var json = (ObjectNode) dtoObjectMapper.readTree(serialized);
        // took and total are the deprecated fields
        assertThat(json, is(jsonObject().where("took", JsonMatchers.jsonLong(searchResponse.getProcessingTime()))));
        assertThat(json, is(jsonObject().where("total", JsonMatchers.jsonLong(searchResponse.getSize()))));
    }

    private SearchResourcesResponse randomResponse() {
        return SearchResourcesResponse.builder()
            .withContext(randomUri())
            .withId(randomUri())
            .withSize(randomInteger())
            .withProcessingTime(randomInteger())
            .withHits(randomJsonList())
            .build();
    }

    private List<JsonNode> randomJsonList() {
        return Stream.of(randomJson(), randomJson())
            .map(attempt(dtoObjectMapper::readTree))
            .flatMap(Try::stream)
            .collect(Collectors.toList());
    }
}
