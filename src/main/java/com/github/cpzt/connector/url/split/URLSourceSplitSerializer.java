package com.github.cpzt.connector.url.split;

import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

public class URLSourceSplitSerializer implements SimpleVersionedSerializer<URLSourceSplit> {
    public static final URLSourceSplitSerializer INSTANCE = new URLSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 1;

    // ------------------------------------------------------------------------

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(URLSourceSplit split) throws IOException {
        checkArgument(
                split.getClass() == URLSourceSplit.class,
                "Cannot serialize subclasses of FileSourceSplit");

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeUTF(split.splitId());
        out.writeUTF(split.url().toString());
        out.writeLong(split.offset());
        out.writeLong(split.length());

        final Optional<Long> readerPosition = split.getReaderPosition();
        out.writeBoolean(readerPosition.isPresent());
        if (readerPosition.isPresent()) {
            out.writeLong(readerPosition.get());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public URLSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final String id = in.readUTF();
        final URL url = new URL(in.readUTF());
        final long offset = in.readLong();
        final long len = in.readLong();

        final Long readerPosition = in.readBoolean() ? in.readLong() : null;

        // instantiate a new split and cache the serialized form
        return new URLSourceSplit(id, url, offset, len, readerPosition, serialized);
    }
}
