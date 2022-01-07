package no.unit.nva.search.restclients.responses;

import static no.unit.nva.testutils.RandomDataGenerator.randomUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import org.junit.jupiter.api.Test;

public class ViewingScopeTest {

    @Test
    void shouldReturnNewViewingScopeWhenSupplyingUris() {
        var desiredUri = randomUri();
        var scope = ViewingScope.create(desiredUri);
        assertThat(scope.getIncludedUnits(), contains(desiredUri));
    }
}