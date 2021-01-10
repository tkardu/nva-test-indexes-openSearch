package no.unit.nva.publication;

import no.unit.nva.model.Publication;
import no.unit.nva.search.exception.ImportException;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JacocoGenerated
public class PublicationClient {

    private final PublicationRestClient publicationRestClient;
    private static final Logger logger = LoggerFactory.getLogger(PublicationClient.class);

    /**
     * Highlevel client to persist Publication, hiding details.
     * @param publicationRestClient actual persister of publication
     */
    public PublicationClient(PublicationRestClient publicationRestClient) {
        this.publicationRestClient = publicationRestClient;
    }

    /**
     * Persist publication som some permanent storage.
     * @param publication to be savend
     * @return persisted publication
     */
    public Publication persist(Publication publication) {
        try {
            return publicationRestClient.upsert(publication);
        } catch (ImportException e) {
            logger.error(e.getMessage(), e);
        }
        return publication;
    }
}
