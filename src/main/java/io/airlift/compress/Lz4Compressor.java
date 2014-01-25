package io.airlift.compress;

import java.util.Arrays;

import static io.airlift.compress.SnappyInternalUtils.copyLong;
import static io.airlift.compress.SnappyInternalUtils.copyMemory;
import static io.airlift.compress.SnappyInternalUtils.loadInt;
import static io.airlift.compress.SnappyInternalUtils.loadLong;
import static io.airlift.compress.SnappyInternalUtils.loadShort;
import static io.airlift.compress.SnappyInternalUtils.writeInt;
import static io.airlift.compress.SnappyInternalUtils.writeShort;
import static net.jpountz.util.UnsafeUtils.writeLong;

public class Lz4Compressor
{
    private final static ControlException EXCEPTION = new ControlException();

    private static final int MAGIC_NUMBER = 0x184D2204;

    private static final int STREAM_DESCRIPTOR_FLAGS_UPPER =
            1 << 6 | // version
                    1 << 5 | // independent blocks
                    0 << 4 | // use checksum
                    1 << 3 ; // has stream size

    private static final int STREAM_DESCRIPTOR_FLAGS_LOWER = 4 << 4; // 64 KB blocks



    private final static int COMPRESSION_LEVEL = 12;
    private final static int NOT_COMPRESSIBLE_CONFIRMATION = 5;

    private final static int HASH_BITS = COMPRESSION_LEVEL + 1;
    private final static int TABLESIZE = 1 << HASH_BITS;

    private final static int COPY_LENGTH = 8;

    private final static int MIN_MATCH = 4;
    private final static int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;

    private static final int HASH_SHIFT = ((MIN_MATCH * 8) - HASH_BITS);

    private final static int LAST_LITERALS = 5;
    private final static int SKIP_STRENGTH = Math.max(2, NOT_COMPRESSIBLE_CONFIRMATION);

    private final static int ML_BITS = 4;
    private final static int ML_MASK = (1 << ML_BITS) - 1;
    private final static int RUN_BITS = 8 - ML_BITS;
    private final static int RUN_MASK = (1 << RUN_BITS) - 1;

    private final static int MIN_LENGTH = MATCH_FIND_LIMIT + 1;

    private final static int BLOCK_SIZE = 1 << 16;

    public static int maxCompressedLength(int sourceLength)
    {
        return (sourceLength + (sourceLength / 255) + 16)
                + 4 // magic
                + 2 // flags
                + 8; // uncompressed size
    }

    public static int compress(
            final byte[] uncompressed,
            final int uncompressedOffset,
            final int uncompressedLength,
            final byte[] compressed,
            final int compressedOffset)
    {
        int compressedIndex = compressedOffset;

        writeInt(compressed, compressedIndex, MAGIC_NUMBER);
        compressedIndex += 4;

        compressed[compressedIndex++] = STREAM_DESCRIPTOR_FLAGS_UPPER;
        compressed[compressedIndex++] = STREAM_DESCRIPTOR_FLAGS_LOWER;

        writeLong(compressed, compressedIndex, uncompressedLength);
        compressedIndex += 8;

        int hashTableSize = TABLESIZE;
        BufferRecycler recycler = BufferRecycler.instance();
        short[] table = recycler.allocEncodingHash(hashTableSize);

        for (int read = 0; read < uncompressedLength; read += BLOCK_SIZE) {
            // Get encoding table for compression
            Arrays.fill(table, (short) 0);

            int sizeIndex = compressedIndex;
            int begin = compressedIndex;
//            int begin = compressedIndex + 4;
            compressedIndex = compressBlock(
                    uncompressed,
                    uncompressedOffset + read,
                    Math.min(uncompressedLength - read, BLOCK_SIZE),
                    compressed,
                    begin,
                    table);

//            SnappyInternalUtils.writeInt(compressed, sizeIndex, Math.min(BLOCK_SIZE, uncompressedLength - read));
//            SnappyInternalUtils.writeInt(compressed, sizeIndex, compressedIndex - begin);
        }

        recycler.releaseEncodingHash(table);

        return compressedIndex - compressedOffset;
    }

    private static int compressBlock(
            final byte[] input,
            final int inputOffset,
            final int inputLength,
            final byte[] output,
            int outputOffset,
            final short[] table)
    {
        int inputIndex = inputOffset;
        int outputIndex = outputOffset;

        int anchor = inputIndex;

        int inputLimit = inputOffset + inputLength;
        int matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
        int matchLimit = inputLimit - LAST_LITERALS;

        if (inputLength < MIN_LENGTH) {
            return emitLastLiteral(output, outputIndex, input, anchor, inputLimit - anchor);
        }

        inputIndex++;
        int nextHash = hash(loadInt(input, inputIndex));

        boolean done = false;
        do {
            int findMatchAttempts = (1 << SKIP_STRENGTH) + 3;

            int nextInputIndex = inputIndex;
            int referenceIndex;

            // find 4-byte match
            do {
                int hash = nextHash;
                inputIndex = nextInputIndex;

                int step = findMatchAttempts++ >> SKIP_STRENGTH;
                nextInputIndex = inputIndex + step;

                if (nextInputIndex > matchFindLimit) {
                    return emitLastLiteral(output, outputIndex, input, anchor, inputLimit - anchor);
                }

                nextHash = hash(loadInt(input, nextInputIndex));
                referenceIndex = inputOffset + (table[hash] & 0xFFFF);
                table[hash] = (short) (inputIndex - inputOffset);
            }
            while (loadInt(input, referenceIndex) != loadInt(input, inputIndex));

            // catch up
            while (inputIndex > anchor && (referenceIndex > inputOffset) && input[inputIndex - 1] == input[referenceIndex - 1]) {
                --inputIndex;
                --referenceIndex;
            }

            int length = inputIndex - anchor;
            int tokenIndex = outputIndex++;

            outputIndex = encodeRunLength(output, outputIndex, tokenIndex, length);

            // emitLiteral copies in chunks of 8 bytes. We are guaranteed to have at least 8 bytes available in output (how?)
            outputIndex = emitLiteral(output, outputIndex, input, anchor, length);

            while (true) {
                outputIndex = encodeOffset(output, outputIndex, inputIndex - referenceIndex);

                // find match length
                inputIndex += MIN_MATCH;
                referenceIndex += MIN_MATCH;
                anchor = inputIndex;

                // first, compare long at a time
                try {
                    while (inputIndex < matchLimit - 7) {
                        long diff = loadLong(input, referenceIndex) ^ loadLong(input, inputIndex);

                        if (diff != 0) {
                            inputIndex += Long.numberOfTrailingZeros(diff) >> 3;
                            throw EXCEPTION;
                        }

                        inputIndex += 8;
                        referenceIndex += 8;
                    }

                    if (inputIndex < matchLimit - 3 && loadInt(input, referenceIndex) == loadInt(input, inputIndex)) {
                        inputIndex += 4;
                        referenceIndex += 4;
                    }

                    if (inputIndex < matchLimit - 1 && loadShort(input, referenceIndex) == loadShort(input, inputIndex)) {
                        inputIndex += 2;
                        referenceIndex += 2;
                    }

                    if (inputIndex < matchLimit && input[referenceIndex] == input[inputIndex]) {
                        ++inputIndex;
                    }
                }
                catch (ControlException e) {
                }

                // Encode MatchLength
                outputIndex = encodeMatchLength(output, outputIndex, tokenIndex, inputIndex - anchor);

                // are we done?
                if (inputIndex > matchFindLimit) {
                    done = true;
                    anchor = inputIndex;
                    break;
                }

                // update hash table to improve match probability
                int z = inputIndex - 2;
                table[hash(loadInt(input, z))] = (short) (z - inputOffset);

                // Test next position
                int x = loadInt(input, inputIndex);
                int y = hash(x);
                referenceIndex = inputOffset + (table[y] & 0xFFFF);
                table[y] = (short) (inputIndex - inputOffset);

                if (loadInt(input, referenceIndex) != x) {
                    // Prepare next major loop
                    anchor = inputIndex++;
                    nextHash = hash(loadInt(input, inputIndex));
                    break;
                }

                // go for another match
                tokenIndex = outputIndex++;
                output[tokenIndex] = 0;
            }
        }
        while (!done);

        // Encode Last Literals
        outputIndex = emitLastLiteral(output, outputIndex, input, anchor, inputLimit - anchor);

        return outputIndex;
    }

    private static int encodeOffset(byte[] output, int outputIndex, int offset)
    {
        writeShort(output, outputIndex, (short) offset);
        return outputIndex + 2;
    }

    private static int emitLiteral(byte[] output, int outputIndex, byte[] input, int inputIndex, int length)
    {
        int outputLimit = outputIndex + length;
        do {
            copyLong(input, inputIndex, output, outputIndex);
            inputIndex += 8;
            outputIndex += 8;
        }
        while (outputIndex < outputLimit);

        return outputLimit;
    }

    private static int encodeMatchLength(byte[] output, int outputIndex, int tokenIndex, int length)
    {
        if (length >= ML_MASK) {
            output[tokenIndex] += ML_MASK;
            length -= ML_MASK;
            while (length > 509) {
                output[outputIndex++] = (byte) 255;
                output[outputIndex++] = (byte) 255;
                length -= 510;
            }
            if (length > 254) {
                length -= 255;
                output[outputIndex++] = (byte) 255;
            }
            output[outputIndex++] = (byte) length;
        }
        else {
            output[tokenIndex] += length;
        }
        return outputIndex;
    }

    private static int encodeRunLength(byte[] output, int outputIndex, int tokenIndex, int length)
    {
        if (length >= RUN_MASK) {
            output[tokenIndex] = (byte) (RUN_MASK << ML_BITS);
            int len = length - RUN_MASK;
            while (len > 254) {
                output[outputIndex++] = (byte) 255;
                len -= 255;
            }
            output[outputIndex++] = (byte) len;
        }
        else {
            output[tokenIndex] = (byte) (length << ML_BITS);
        }

        return outputIndex;
    }

    private static int emitLastLiteral(byte[] output, int outputIndex, byte[] input, int inputIndex, int length)
    {
        outputIndex = encodeRunLength(output, outputIndex + 1, outputIndex, length);
        copyMemory(input, inputIndex, output, outputIndex, length);

        return outputIndex + length;
    }

    private static int hash(int value)
    {
        return (int) ((value * 2654435761L) & 0xFFFFFFFFL) >>> HASH_SHIFT;
    }

    private static int writeUncompressedLength(byte[] output, int outputIndex, int uncompressedLength)
    {
        int highBitMask = 0x80;
        if (uncompressedLength < (1 << 7) && uncompressedLength >= 0) {
            output[outputIndex++] = (byte) (uncompressedLength);
        }
        else if (uncompressedLength < (1 << 14) && uncompressedLength > 0) {
            output[outputIndex++] = (byte) (uncompressedLength | highBitMask);
            output[outputIndex++] = (byte) (uncompressedLength >>> 7);
        }
        else if (uncompressedLength < (1 << 21) && uncompressedLength > 0) {
            output[outputIndex++] = (byte) (uncompressedLength | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 7) | highBitMask);
            output[outputIndex++] = (byte) (uncompressedLength >>> 14);
        }
        else if (uncompressedLength < (1 << 28) && uncompressedLength > 0) {
            output[outputIndex++] = (byte) (uncompressedLength | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 7) | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 14) | highBitMask);
            output[outputIndex++] = (byte) (uncompressedLength >>> 21);
        }
        else {
            output[outputIndex++] = (byte) (uncompressedLength | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 7) | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 14) | highBitMask);
            output[outputIndex++] = (byte) ((uncompressedLength >>> 21) | highBitMask);
            output[outputIndex++] = (byte) (uncompressedLength >>> 28);
        }
        return outputIndex;
    }

    private final static class ControlException extends Exception {}
}
