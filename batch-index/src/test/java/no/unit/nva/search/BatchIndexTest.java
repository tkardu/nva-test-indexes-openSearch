package no.unit.nva.search;

import static org.mockito.Mockito.mock;
import com.amazonaws.services.lambda.runtime.Context;
import java.nio.file.Path;
import no.unit.nva.search.exception.SearchException;
import no.unit.nva.testutils.IoUtils;

public class BatchIndexTest {

    public static final String INPUT_1 = "input1.ion.gz";
    public static final String INPUT_2 = "input2.ion.gz";
    public static final String INPUT_3 = "input3.ion.gz";
    public static final String INPUT_4 = "input4.ion.gz";
    public static final String[] RESOURCES = {INPUT_1, INPUT_2, INPUT_3, INPUT_4};
    public static final String EXPECTED_EXCEPTION_MESSAGE = "expectedMessage";

    public static final String FILE_WITH_IDENTIFIERS_OF_PUBLISHED_RESOURCES_IN_SAMPLE_FILES =
        "published_resources_identifiers_for_all_files.txt";
    public static final Context CONTEXT = mock(Context.class);
    public static final String[] PUBLISHED_RESOURCES_IDENTIFIERS = readPublishedResourcesIdentifiersFromStaticFile();

    protected StubElasticSearchHighLevelRestClient failingElasticSearchClient() {
        return new StubElasticSearchHighLevelRestClient() {
            @Override
            public void addDocumentToIndex(IndexDocument document) throws SearchException {
                throw new SearchException(document.getId().toString(),
                                          new RuntimeException(EXPECTED_EXCEPTION_MESSAGE));
            }
        };
    }

    private static String[] readPublishedResourcesIdentifiersFromStaticFile() {
        return IoUtils.linesfromResource(Path.of(FILE_WITH_IDENTIFIERS_OF_PUBLISHED_RESOURCES_IN_SAMPLE_FILES))
            .toArray(String[]::new);
    }
}
