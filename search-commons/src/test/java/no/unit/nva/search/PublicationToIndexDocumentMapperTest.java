package no.unit.nva.search;

import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValuesIgnoringFields;
import static org.hamcrest.MatcherAssert.assertThat;
import java.net.MalformedURLException;
import java.util.Set;
import no.unit.nva.model.Publication;
import no.unit.nva.model.exceptions.InvalidIssnException;
import no.unit.nva.publication.PublicationGenerator;
import org.junit.jupiter.api.Test;

class PublicationToIndexDocumentMapperTest {

    public static final Set<String> IGNORED_PUBLICATION_FIELDS = Set.of("doiRequest");
    public static final Set<String> IGNORED_INDEXED_DOCUMENT_FIELDS = Set.of("publisher.name");

    @Test
    public  void toIndexDocumentCreatesReturnsNewIndexDocumentWithNoMissingFields()
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        assertThat(publication,doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        PublicationToIndexDocumentMapper mapper=  new PublicationToIndexDocumentMapper(publication);
        IndexDocument actualDocument = mapper.generateIndexDocument();
        assertThat(actualDocument,doesNotHaveEmptyValuesIgnoringFields(IGNORED_INDEXED_DOCUMENT_FIELDS));
    }


}