package no.unit.nva.search;


class IndexDocumentWrapperLinkedDataTest {

/*

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

        String jsonLd = actualDocument.toJsonLdString();
        assertNotNull(jsonLd);
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
        String jsonLd = actualDocument.toJsonString();
        assertNotNull(jsonLd);
    }

    @Test
    public void toFramedJsonLdProducesJsonLdIncludingReferencedData() throws Exception {
        final URI bookSeriesId = randomPublicationChannelsUri();
        final URI publisherId = randomPublicationChannelsUri();
        EntityDescription entityDescription = PublicationGenerator.createSampleEntityDescriptionBook(bookSeriesId, publisherId);

        Publication publication = createPublicationWithEntityDescription(randomPublicationChannelsUri(), entityDescription);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);

        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSample =
                IoUtils.stringFromResources(Path.of("framed-json/publication_channel_sample.json"));
        when(mockUriRetriever.getRawContent(any(), any())).thenReturn(publicationChannelSample);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(mockUriRetriever,
                PublicationChannelsFilter::isPublicationChannelUri).toFramedJsonLd(indexDocument);
        assertNotNull(framedJsonLd);

    }

    @Test
    public void toFramedJsonLdProducesJsonLdIncludingReferencedDataWhenInputIsBookWithBookSeries() throws Exception {
        final URI bookSeriesId = randomPublicationChannelsUri();
        final URI publisherId = randomPublicationChannelsUri();
        EntityDescription entityDescription = PublicationGenerator.createSampleEntityDescriptionBook(bookSeriesId, publisherId);
        Publication publication = createPublicationWithEntityDescription(randomPublicationChannelsUri(), entityDescription);
        IndexDocument indexDocument = IndexDocument.fromPublication(publication);

        final UriRetriever mockUriRetriever = mock(UriRetriever.class);
        String publicationChannelSample =
                IoUtils.stringFromResources(Path.of("framed-json/publication_channel_sample.json"));
        when(mockUriRetriever.getRawContent(any(), any())).thenReturn(publicationChannelSample);
        final String framedJsonLd = new IndexDocumentWrapperLinkedData(mockUriRetriever,
                PublicationChannelsFilter::isPublicationChannelUri).toFramedJsonLd(indexDocument);
        assertNotNull(framedJsonLd);
    }

    @Test
    public void toFramedJsonLdIncludesReferencedDataWhenInputIsJournalWithPublicationChannelUriAsId() throws Exception {
        fail("not implemented");
    }

    @Test
    public void toFramedJsonLdSkipsReferencedDataWhenInputIsJournalWithStringAsId() throws Exception {
        fail("not implemented");
    }

    @Test
    public void toFramedJsonLdIncludesReferencedDataWhenInputIsReportWithPublicationChannelUrisAsIdentifier() throws Exception {
        fail("not implemented");
    }

    @Test
    public void toFramedJsonLdIncludesReferencedDataWhenInputIsDegreeWithPublicationChannelUrisAsIdentifier() throws Exception {
        fail("not implemented");
    }

    @Test
    public void toFramedJsonLdSkipsReferencedDataWhenInputIsUnconfirmedJournal() throws Exception {
        fail("not implemented");
    }


    private Publication createPublicationWithEntityDescription(URI uri, EntityDescription entityDescription) {

        Publication publication = PublicationGenerator
                .generatePublicationWithEntityDescription(SortableIdentifier.next(), entityDescription);

        return publication;
    }
*/


}
