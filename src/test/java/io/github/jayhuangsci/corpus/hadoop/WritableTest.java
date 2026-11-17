package io.github.jayhuangsci.corpus.hadoop;

import io.github.jayhuangsci.corpus.core.PipelineOptions;
import io.github.jayhuangsci.corpus.core.TextProcessor;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import static org.junit.jupiter.api.Assertions.*;

class WritableTest {
    private static TextProcessor.Result result() {
        return TextProcessor.process("Distributed systems process readable documents while preserving data quality and deterministic results.",
                PipelineOptions.parse(new String[]{"--input", "i", "--output", "o"}));
    }

    @Test void writableRoundTripsWithoutAliasing() throws Exception {
        PreparedDocumentWritable source = new PreparedDocumentWritable(result(), "source", 123);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(); source.write(new DataOutputStream(buffer));
        PreparedDocumentWritable copy = new PreparedDocumentWritable();
        copy.readFields(new DataInputStream(new ByteArrayInputStream(buffer.toByteArray())));
        assertEquals(source.text(), copy.text()); assertEquals(123, copy.sourceOffset());
        assertEquals(source.redactions(), copy.redactions()); assertEquals(1, copy.occurrences());
    }

    @Test void dedupMergeIsAssociativeWithDeterministicProvenance() throws Exception {
        PreparedDocumentWritable a = new PreparedDocumentWritable(result(), "z", 12);
        PreparedDocumentWritable b = new PreparedDocumentWritable(result(), "a", 25);
        PreparedDocumentWritable c = new PreparedDocumentWritable(result(), "a", 5);
        PreparedDocumentWritable left = a.merge(b).merge(c), right = a.merge(b.merge(c));
        assertEquals(3, left.occurrences()); assertEquals(left.occurrences(), right.occurrences());
        assertEquals("a", left.sourceId()); assertEquals(5, left.sourceOffset());
        assertEquals(left.sourceOffset(), right.sourceOffset()); assertEquals(1, a.occurrences());
    }
}
