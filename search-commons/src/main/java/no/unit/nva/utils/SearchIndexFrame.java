package no.unit.nva.utils;

import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;

import java.io.InputStream;
import java.nio.file.Path;

@JacocoGenerated
public class SearchIndexFrame {

    public static final String FRAME_JSON = "framed-json/test_frame.json";
    private final String frameSrc;

    public SearchIndexFrame() {
        frameSrc = IoUtils.stringFromResources(Path.of(FRAME_JSON));
    }

    public InputStream asInputStream() {
        return IoUtils.stringToStream(frameSrc);
    }

}
