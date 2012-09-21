package io.airlift.compress;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static io.airlift.compress.SnappyInternalUtils.copyInt;
import static io.airlift.compress.SnappyInternalUtils.copyLong;
import static io.airlift.compress.SnappyInternalUtils.copyMemory;
import static io.airlift.compress.SnappyInternalUtils.loadByte;
import static io.airlift.compress.SnappyInternalUtils.loadShort;

public class Lz4DirectMemoryDecompressor
{
    private static final Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);

            int byteArrayIndexScale = unsafe.arrayIndexScale(byte[].class);
            if (byteArrayIndexScale != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + byteArrayIndexScale);
            }
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


    private final static int[] DEC_TABLE_1 = { 0, 3, 2, 3, 0, 0, 0, 0 };
    private final static int[] DEC_TABLE_2 = { 0, 0, 0, -1, 0, 1, 2, 3 };

    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
            throws CorruptionException
    {
        return readUncompressedLength(compressed, compressedOffset)[0];
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
            throws CorruptionException
    {
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

        // allocate the uncompressed buffer
        byte[] uncompressed = new byte[expectedLength];

        // Process the entire input
        int uncompressedSize = decompressAllChunks(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                0);

        if (!(expectedLength == uncompressedSize)) {
            throw new CorruptionException(String.format("Recorded length is %s bytes but actual length after decompression is %s bytes ",
                    expectedLength,
                    uncompressedSize));
        }

        return uncompressed;
    }

    public static int uncompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

        SnappyInternalUtils.checkArgument(expectedLength <= uncompressed.length - uncompressedOffset,
                "Uncompressed length %s must be less than %s", expectedLength, uncompressed.length - uncompressedOffset);

        // Process the entire input
        int uncompressedSize = decompressAllChunks(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                uncompressedOffset);

        if (!(expectedLength == uncompressedSize)) {
            throw new CorruptionException(String.format("Recorded length is %s bytes but actual length after decompression is %s bytes ",
                    expectedLength,
                    uncompressedSize));
        }

        return expectedLength;
    }

    private static int decompressAllChunks(
            final byte[] input,
            final int inputOffset,
            final int inputSize,
            final byte[] output,
            final int outputOffset)
    {
        int outputIndex = outputOffset;
        int inputIndex = inputOffset;

        while (inputIndex < inputOffset + inputSize) {
            int chunkSize = SnappyInternalUtils.loadInt(input, inputIndex);
            inputIndex += 4;

            outputIndex = decompressChunk(input, inputIndex, chunkSize, output, outputIndex);
            inputIndex += chunkSize;
        }

        return outputIndex - outputOffset;
    }

    public static int decompressChunk(
            final byte[] input,
            final int inputOffset,
            final int inputSize,
            final byte[] output,
            final int outputOffset)
            throws CorruptionException
    {
        int inputIndex = inputOffset;
        int outputIndex = outputOffset;

        int inputLimit = inputOffset + inputSize;
        int fastInputLimit = inputLimit - 8; // maximum offset in input buffer from which it's safe to read long-at-a-time
        int fastOutputLimit = output.length - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time

        while (inputIndex < inputLimit) {
            int token = loadByte(input, inputIndex++);

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xf) {
                while (loadByte(input, inputIndex) == 255) {
                    literalLength += 255;
                    inputIndex++;
                }
                literalLength += loadByte(input, inputIndex++);
            }

            // copy literal
            int literalOutputLimit = outputIndex + literalLength;
            if (literalOutputLimit > fastOutputLimit || inputIndex + literalLength > fastInputLimit) {
                // slow, precise copy
                copyMemory(input, inputIndex, output, outputIndex, literalLength);
                return literalOutputLimit; //outputIndex + literalLength;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            do {
                copyLong(input, inputIndex, output, outputIndex);
                inputIndex += 8;
                outputIndex += 8;
            }
            while (outputIndex < literalOutputLimit);
            inputIndex -= (outputIndex - literalOutputLimit); // adjust index if we overcopied
            outputIndex = literalOutputLimit;

            // get offset
            int offset = loadShort(input, inputIndex);
            inputIndex += 2;

            // get matchlength
            int matchLength = token & 0xf; // bottom-most 4 bits of token
            if (matchLength == 0xf) {
                while (loadByte(input, inputIndex) == 255) {
                    matchLength += 255;
                    inputIndex++;
                }
                matchLength += loadByte(input, inputIndex++);
            }
            matchLength += 4; // implicit length from initial 4-byte match in encoder

            // copy repeated sequence
            int sourceIndex = outputIndex - offset;
            if (offset < 8) {
                // copies 8 bytes from sourceIndex to destIndex and leaves the pointers more than
                // 8 bytes apart so that we can copy long-at-a-time below
                int dec1 = DEC_TABLE_1[offset];

                output[outputIndex++] = output[sourceIndex++];
                output[outputIndex++] = output[sourceIndex++];
                output[outputIndex++] = output[sourceIndex++];
                output[outputIndex++] = output[sourceIndex++];
                sourceIndex -= dec1;

                copyInt(output, sourceIndex, output, outputIndex);
                outputIndex += 4;
                sourceIndex -= DEC_TABLE_2[offset];
            }
            else {
                copyLong(output, sourceIndex, output, outputIndex);
                outputIndex += 8;
                sourceIndex += 8;
            }

            // subtract 8 for the bytes we copied above
            int matchOutputLimit = outputIndex + matchLength - 8;

            if (matchOutputLimit > fastOutputLimit) {
                while (outputIndex < fastOutputLimit) {
                    copyLong(output, sourceIndex, output, outputIndex);
                    sourceIndex += 8;
                    outputIndex += 8;
                }

                while (outputIndex < matchOutputLimit) {
                    output[outputIndex++] = output[sourceIndex++];
                }

                if (outputIndex == output.length) { // Check EOF (should never happen, since last 5 bytes are supposed to be literals)
                    return outputIndex;
                }
            }

            while (outputIndex < matchOutputLimit) {
                copyLong(output, sourceIndex, output, outputIndex);
                sourceIndex += 8;
                outputIndex += 8;
            }

            outputIndex = matchOutputLimit; // correction in case we overcopied
        }

        return outputIndex;
    }

    /**
     * Reads the variable length integer encoded a the specified offset, and returns this length with the number of bytes read.
     */
    private static int[] readUncompressedLength(byte[] compressed, int compressedOffset)
            throws CorruptionException
    {
        int result;
        int bytesRead = 0;
        {
            int b = compressed[compressedOffset + bytesRead++] & 0xFF;
            result = b & 0x7f;
            if ((b & 0x80) != 0) {
                b = compressed[compressedOffset + bytesRead++] & 0xFF;
                result |= (b & 0x7f) << 7;
                if ((b & 0x80) != 0) {
                    b = compressed[compressedOffset + bytesRead++] & 0xFF;
                    result |= (b & 0x7f) << 14;
                    if ((b & 0x80) != 0) {
                        b = compressed[compressedOffset + bytesRead++] & 0xFF;
                        result |= (b & 0x7f) << 21;
                        if ((b & 0x80) != 0) {
                            b = compressed[compressedOffset + bytesRead++] & 0xFF;
                            result |= (b & 0x7f) << 28;
                            if ((b & 0x80) != 0) {
                                throw new CorruptionException("last byte of compressed length int has high bit set");
                            }
                        }
                    }
                }
            }
        }
        return new int[] { result, bytesRead };
    }

}
