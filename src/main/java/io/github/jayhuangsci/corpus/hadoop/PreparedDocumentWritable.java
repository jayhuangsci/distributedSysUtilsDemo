package io.github.jayhuangsci.corpus.hadoop;

import io.github.jayhuangsci.corpus.core.TextProcessor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/** Associative deduplication state with deterministic representative provenance. */
public final class PreparedDocumentWritable implements Writable {
    private String text = "", sourceId = "";
    private long sourceOffset, occurrences;
    private Map<String, Integer> redactions = new TreeMap<>();

    public PreparedDocumentWritable() { }
    public PreparedDocumentWritable(TextProcessor.Result result, String sourceId, long sourceOffset) {
        text = result.text; this.sourceId = sourceId; this.sourceOffset = sourceOffset;
        occurrences = 1; redactions = new TreeMap<>(result.redactions);
    }

    public PreparedDocumentWritable copy() {
        PreparedDocumentWritable copy = new PreparedDocumentWritable();
        copy.text = text; copy.sourceId = sourceId; copy.sourceOffset = sourceOffset;
        copy.occurrences = occurrences; copy.redactions = new TreeMap<>(redactions);
        return copy;
    }

    public PreparedDocumentWritable merge(PreparedDocumentWritable other) throws IOException {
        if (!text.equals(other.text)) { throw new IOException("SHA-256 collision or corrupt deduplication state"); }
        long count = Math.addExact(occurrences, other.occurrences);
        int sourceOrder = sourceId.compareTo(other.sourceId);
        PreparedDocumentWritable representative = sourceOrder > 0 || (sourceOrder == 0 && sourceOffset > other.sourceOffset) ? other : this;
        PreparedDocumentWritable result = representative.copy();
        result.occurrences = count;
        return result;
    }

    public String text() { return text; }
    public String sourceId() { return sourceId; }
    public long sourceOffset() { return sourceOffset; }
    public long occurrences() { return occurrences; }
    public Map<String, Integer> redactions() { return new TreeMap<>(redactions); }

    @Override public void write(DataOutput out) throws IOException {
        out.writeInt(1); Text.writeString(out, text); Text.writeString(out, sourceId);
        out.writeLong(sourceOffset); out.writeLong(occurrences);
        out.writeInt(redactions.size());
        for (Map.Entry<String, Integer> entry : redactions.entrySet()) { Text.writeString(out, entry.getKey()); out.writeInt(entry.getValue()); }
    }

    @Override public void readFields(DataInput in) throws IOException {
        if (in.readInt() != 1) { throw new IOException("Unsupported prepared-document schema version"); }
        text = Text.readString(in); sourceId = Text.readString(in);
        sourceOffset = in.readLong(); occurrences = in.readLong();
        redactions = new TreeMap<>(); int count = in.readInt();
        if (count < 0 || count > 5) { throw new IOException("Invalid redaction metadata"); }
        for (int i = 0; i < count; i++) { redactions.put(Text.readString(in), in.readInt()); }
    }
}
