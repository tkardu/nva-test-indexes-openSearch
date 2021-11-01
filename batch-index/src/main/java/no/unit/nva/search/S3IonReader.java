package no.unit.nva.search;

import com.fasterxml.jackson.databind.JsonNode;
import nva.commons.core.attempt.Try;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonReaderBuilder;
import software.amazon.ion.system.IonTextWriterBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static no.unit.nva.search.constants.ApplicationConstants.objectMapperWithEmpty;
import static nva.commons.core.attempt.Try.attempt;

public final class S3IonReader {

    private S3IonReader() {

    }

    public static Stream<JsonNode> extractJsonNodesFromIonContent(InputStream content) {
        return
            contentToLines(content)
                .map(attempt(S3IonReader::toJsonObjectsString))
                .map(attempt -> attempt.map(S3IonReader::toJsonNode))
                .map(Try::orElseThrow);
    }

    private static JsonNode toJsonNode(String jsonString) {
        return attempt(() -> objectMapperWithEmpty.readTree(jsonString)).orElseThrow();
    }

    private static Stream<String> contentToLines(InputStream content) {
        return new BufferedReader(new InputStreamReader(content)).lines();
    }

    private static String toJsonObjectsString(String ion) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try (IonWriter writer = createIonToJsonTransformer(stringBuilder)) {
            rewrite(ion, writer);
        }
        return stringBuilder.toString();
    }

    private static IonWriter createIonToJsonTransformer(StringBuilder stringBuilder) {
        return IonTextWriterBuilder.json().withCharset(StandardCharsets.UTF_8).build(stringBuilder);
    }

    private static void rewrite(String textIon, IonWriter writer) throws IOException {
        try (IonReader reader = IonReaderBuilder.standard().build(textIon)) {
            writer.writeValues(reader);
        }
    }
}
