package no.unit.nva.search;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import no.unit.nva.model.EntityDescription;
import no.unit.nva.model.Publication;
import no.unit.nva.model.Reference;
import no.unit.nva.model.instancetypes.PublicationInstance;
import no.unit.nva.search.IndexDocument.Builder;

class PublicationToIndexDocumentMapper {

    private final Publication publication;

    protected PublicationToIndexDocumentMapper(Publication publication) {
        this.publication = publication;
    }

   protected IndexDocument generateIndexDocument() {
        return new Builder()
                   .withId(publication.getIdentifier())
                   .withTitle(extractTitle())
                   .withDoi(publication.getDoi())
                   .withAbstract(extractAbstract())
                   .withContributors(extractContributors())
                   .withDescription(extractDescription())
                   .withModifiedDate(publication.getModifiedDate())
                   .withOwner(publication.getOwner())
                   .withPublisher(IndexPublisher.fromPublisher(publication.getPublisher()))
                   .withAlternativeTitles(extractAlternativeTitles())
                   .withPublicationDate(extractDate())
                   .withType(extractPublicationType())
                   .withPublishedDate(publication.getPublishedDate())
                   .withReference(extractReference())
                   .withTags(extractTags())
                   .build();
    }

    private String extractTitle() {
        return entityDescription().map(EntityDescription::getMainTitle).orElse(null);
    }

    private String extractAbstract() {
        return entityDescription().map(EntityDescription::getAbstract).orElse(null);
    }

    private List<IndexContributor> extractContributors() {
        return entityDescription()
                   .stream()
                   .map(EntityDescription::getContributors)
                   .flatMap(Collection::stream)
                   .map(IndexContributor::fromContributor)
                   .collect(Collectors.toList());
    }

    private String extractDescription() {
        return entityDescription().map(EntityDescription::getDescription).orElse(null);
    }

    private Map<String, String> extractAlternativeTitles() {
        return entityDescription().map(EntityDescription::getAlternativeTitles).orElse(Collections.emptyMap());
    }

    private IndexDate extractDate() {
        return entityDescription()
                   .map(EntityDescription::getDate)
                   .map(IndexDate::fromDate)
                   .orElse(null);
    }

    private String extractPublicationType() {
        return entityDescription()
                   .map(EntityDescription::getReference)
                   .map(Reference::getPublicationInstance)
                   .map(PublicationInstance::getInstanceType)
                   .orElse(null);
    }

    private Reference extractReference() {
        return entityDescription().map(EntityDescription::getReference).orElse(null);
    }

    private List<String> extractTags() {
        return entityDescription().map(EntityDescription::getTags).orElse(Collections.emptyList());
    }

    private Optional<EntityDescription> entityDescription() {
        return Optional.of(publication).map(Publication::getEntityDescription);
    }
}
