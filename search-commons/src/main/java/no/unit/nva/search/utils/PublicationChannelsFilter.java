package no.unit.nva.search.utils;

import nva.commons.core.JacocoGenerated;

import java.net.URI;

@JacocoGenerated
public class PublicationChannelsFilter {

    public static final String PUBLICATION_CHANNELS = "publication-channels";

    public static boolean isPublicationChannelUri(URI uri) {
        return uri.getPath().contains(PUBLICATION_CHANNELS);
    }
}
