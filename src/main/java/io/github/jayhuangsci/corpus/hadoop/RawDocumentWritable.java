package io.github.jayhuangsci.corpus.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class RawDocumentWritable implements Writable {
    private final Text bytes = new Text();
    private boolean oversized;

    public void set(Text value, boolean oversized) {
        this.oversized = oversized;
        if (oversized) { bytes.clear(); } else { bytes.set(value); }
    }

    public boolean oversized() { return oversized; }

    public String decode() throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes.getBytes(), 0, bytes.getLength())).toString();
    }

    @Override public void write(DataOutput out) throws IOException { out.writeBoolean(oversized); bytes.write(out); }
    @Override public void readFields(DataInput in) throws IOException { oversized = in.readBoolean(); bytes.readFields(in); }
}
