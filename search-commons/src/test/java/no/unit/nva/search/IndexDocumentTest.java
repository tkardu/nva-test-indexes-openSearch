package no.unit.nva.search;

import no.unit.nva.model.Contributor;
import no.unit.nva.model.EntityDescription;
import no.unit.nva.model.Publication;
import no.unit.nva.model.exceptions.InvalidIsbnException;
import no.unit.nva.model.exceptions.InvalidIssnException;
import no.unit.nva.model.exceptions.InvalidUnconfirmedSeriesException;
import no.unit.nva.publication.PublicationGenerator;
import nva.commons.core.attempt.Try;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValues;
import static no.unit.nva.hamcrest.DoesNotHaveEmptyValues.doesNotHaveEmptyValuesIgnoringFields;
import static no.unit.nva.publication.PublicationGenerator.randomPublicationChannelsUri;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;

class IndexDocumentTest {

    public static final Set<String> IGNORED_PUBLICATION_FIELDS = Set.of("doiRequest", "subjects");
    public static final Set<String> IGNORED_INDEXED_DOCUMENT_FIELDS = Set.of("publisher.name");

    @Test
    public void toIndexDocumentCreatesReturnsNewIndexDocumentWithNoMissingFields()
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertThat(actualDocument, doesNotHaveEmptyValuesIgnoringFields(IGNORED_INDEXED_DOCUMENT_FIELDS));
    }

    @Test
    public void toIndexDocumentReturnsIndexDocumentWithAllContributorsWhenPublicationHasManyContributors()
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        List<Contributor> contributors = IntStream.range(0, 100).boxed()
                                             .map(attempt(PublicationGenerator::randomContributor))
                                             .map(Try::orElseThrow)
                                             .collect(Collectors.toList());

        publication.getEntityDescription().setContributors(contributors);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);

        List<IndexContributor> indexContributors = indexDocument.getContributors();

        for (int sequence = 0; sequence < indexContributors.size(); sequence++) {
            Contributor sourceContributor = contributors.get(sequence);
            IndexContributor indexContributor = indexContributors.get(sequence);
            assertThatIndexContributorHasCorrectData(sourceContributor, indexContributor, sequence);
        }
    }

    @Test
    public void toIndexDocumentReturnsIndexDocumentWitEmptyContributorListWhenPublicationHasNoContributors()
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();

        publication.getEntityDescription().setContributors(Collections.emptyList());
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);
        List<IndexContributor> indexContributors = indexDocument.getContributors();
        assertThat(indexContributors, is(empty()));
    }

    @Test
    public void toIndexDocumentReturnsIndexDocumentWithNoDateWithPublicationDateIsNull()
        throws MalformedURLException, InvalidIssnException {
        Publication publication = PublicationGenerator.publicationWithIdentifier();
        publication.getEntityDescription().setDate(null);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);
        assertThat(indexDocument.getPublicationDate(),is(nullValue()));
    }


    @Test
    void toJsonStringSerializesSingleIndexDocumentToValidJsonLdWithContext()
        // Publication uten referanse til publication-channels
            throws InvalidIsbnException, InvalidUnconfirmedSeriesException {

        final URI bookSeriesId = randomPublicationChannelsUri();
        final URI publisherId = randomPublicationChannelsUri();
        EntityDescription entityDescription = PublicationGenerator.createSampleEntityDescriptionBook(bookSeriesId, publisherId);

        Publication publication = createPublicationWithEntityDescription(randomPublicationChannelsUri(), entityDescription);
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertThat(actualDocument, doesNotHaveEmptyValuesIgnoringFields(IGNORED_INDEXED_DOCUMENT_FIELDS));

    }

    @Test
    public void toJsonStringDereferencesUris() throws InvalidIsbnException, InvalidUnconfirmedSeriesException {
        final URI bookSeriesId = randomPublicationChannelsUri();
        final URI publisherId = randomPublicationChannelsUri();
        EntityDescription entityDescription = PublicationGenerator.createSampleEntityDescriptionBook(bookSeriesId, publisherId);

        Publication publication = createPublicationWithEntityDescription(randomPublicationChannelsUri(), entityDescription);
        assertThat(publication, doesNotHaveEmptyValuesIgnoringFields(IGNORED_PUBLICATION_FIELDS));
        IndexDocument actualDocument = IndexDocument.fromPublication(publication);
        assertThat(actualDocument, doesNotHaveEmptyValuesIgnoringFields(IGNORED_INDEXED_DOCUMENT_FIELDS));
    }

    @Test
    public void toFramedJsonLdProducesJsonLdIncludingReferencedData() throws Exception {
        final URI bookSeriesId = randomPublicationChannelsUri();
        final URI publisherId = randomPublicationChannelsUri();
        EntityDescription entityDescription = PublicationGenerator.createSampleEntityDescriptionBook(bookSeriesId, publisherId);

        Publication publication = createPublicationWithEntityDescription(randomPublicationChannelsUri(), entityDescription);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);


    }


    private void assertThatIndexContributorHasCorrectData(Contributor sourceContributor,
                                                          IndexContributor indexContributor,
                                                          int sequence) {

        assertThat(indexContributor, doesNotHaveEmptyValues());
        assertThat(indexContributor.getName(), is(equalTo(sourceContributor.getIdentity().getName())));
        assertThat(indexContributor.getId(), is(equalTo(sourceContributor.getIdentity().getId())));
        assertThat(sequence, is(equalTo(sourceContributor.getSequence())));
    }
}
