package com.github.cpzt.connector.url.enumerator.state;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.flink.core.io.SimpleVersionedSerializer;


public class PendingSplitsStateSerializer
        implements SimpleVersionedSerializer<PendingSplitsState> {
    private static final int VERSION = 1;

    private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

    private final SimpleVersionedSerializer<URLSourceSplit> splitSerializer;

    public PendingSplitsStateSerializer(
            SimpleVersionedSerializer<URLSourceSplit> splitSerializer) {
        this.splitSerializer = checkNotNull(splitSerializer);
    }

    // ------------------------------------------------------------------------
    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsState state) throws IOException {
        checkArgument(
                state.getClass() == PendingSplitsState.class,
                "Cannot serialize subclasses of PendingSplitsState");

        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }

        final SimpleVersionedSerializer<URLSourceSplit> splitSerializer =
                this.splitSerializer; // stack cache
        final Collection<URLSourceSplit> splits = state.getSplits();
        final Collection<URL> processedURLs = state.getAlreadyProcessedURLs();

        final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
        final ArrayList<byte[]> serializedPaths = new ArrayList<>(processedURLs.size());

        int totalLen =
                16; // four ints: magic, version of split serializer, count splits, count paths

        for (URLSourceSplit split : splits) {
            final byte[] serSplit = splitSerializer.serialize(split);
            serializedSplits.add(serSplit);
            totalLen += serSplit.length + 4; // 4 bytes for the length field
        }

        for (URL url : processedURLs) {
            final byte[] serPath = url.toString().getBytes(StandardCharsets.UTF_8);
            serializedPaths.add(serPath);
            totalLen += serPath.length + 4; // 4 bytes for the length field
        }

        final byte[] result = new byte[totalLen];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
        byteBuffer.putInt(splitSerializer.getVersion());
        byteBuffer.putInt(serializedSplits.size());
        byteBuffer.putInt(serializedPaths.size());

        for (byte[] splitBytes : serializedSplits) {
            byteBuffer.putInt(splitBytes.length);
            byteBuffer.put(splitBytes);
        }

        for (byte[] pathBytes : serializedPaths) {
            byteBuffer.putInt(pathBytes.length);
            byteBuffer.put(pathBytes);
        }

        assert byteBuffer.remaining() == 0;

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        state.serializedFormCache = result;

        return result;
    }

    @Override
    public PendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        final int magic = bb.getInt();
        if (magic != VERSION_1_MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Invalid magic number for PendingSplitsCheckpoint. "
                                    + "Expected: %X , found %X",
                            VERSION_1_MAGIC_NUMBER, magic));
        }

        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numURLs = bb.getInt();

        final SimpleVersionedSerializer<URLSourceSplit> splitSerializer =
                this.splitSerializer; // stack cache
        final ArrayList<URLSourceSplit> splits = new ArrayList<>(numSplits);
        final ArrayList<URL> urls = new ArrayList<>(numURLs);

        for (int remaining = numSplits; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final URLSourceSplit split =
                    splitSerializer.deserialize(splitSerializerVersion, bytes);
            splits.add(split);
        }

        for (int remaining = numURLs; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final URL url = new URL(new String(bytes, StandardCharsets.UTF_8));
            urls.add(url);
        }

        return PendingSplitsState.reusingCollection(splits, urls);
    }
}
