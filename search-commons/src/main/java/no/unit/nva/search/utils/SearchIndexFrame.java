package no.unit.nva.search.utils;

import nva.commons.core.ioutils.IoUtils;

import java.io.InputStream;
import java.nio.file.Path;

public class SearchIndexFrame {

    public static final String FRAME_JSON = "publication_frame.json";
    private final String frameSrc;

    public SearchIndexFrame() {
        frameSrc = IoUtils.stringFromResources(Path.of(FRAME_JSON));
    }

    public InputStream asInputStream() {
        return IoUtils.stringToStream(frameSrc);
    }

}
