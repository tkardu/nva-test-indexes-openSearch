package no.unit.nva.publication;

import no.unit.nva.model.Publication;
import no.unit.nva.search.exception.ImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublicationClient {

    private final PublicationRestClient publicationRestClient;
    private static final Logger logger = LoggerFactory.getLogger(PublicationClient.class);

    public PublicationClient(PublicationRestClient publicationRestClient) {
        this.publicationRestClient = publicationRestClient;
    }

    public Publication persist(Publication publication) {
        try {
            return publicationRestClient.upsert(publication);
        } catch (ImportException e) {
            logger.error(e.getMessage(), e);
        }
        return publication;
    }
}
