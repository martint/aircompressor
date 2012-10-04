package io.airlift.compress;

import io.airlift.compress.slice.UnsafeSlice;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static io.airlift.compress.SnappyInternalUtils.copyInt;
import static io.airlift.compress.SnappyInternalUtils.copyLong;
import static io.airlift.compress.SnappyInternalUtils.copyMemory;
import static io.airlift.compress.SnappyInternalUtils.loadByte;
import static io.airlift.compress.SnappyInternalUtils.loadShort;
import static io.airlift.compress.UnsafeMemory.copyByte;
import static io.airlift.compress.UnsafeMemory.readByte;
import static io.airlift.compress.UnsafeMemory.readInt;
import static io.airlift.compress.UnsafeMemory.readShort;

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

    public static int getUncompressedLength(UnsafeSlice compressed, int compressedOffset)
            throws CorruptionException
    {
        return (int) readUncompressedLength(compressed.getAddress() + compressedOffset)[0];
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
            throws CorruptionException
    {
        //        // Read the uncompressed length from the front of the compressed input
        //        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        //        int expectedLength = varInt[0];
        //        compressedOffset += varInt[1];
        //        compressedSize -= varInt[1];
        //
        //        // allocate the uncompressed buffer
        //        byte[] uncompressed = new byte[expectedLength];
        //
        //        // Process the entire input
        //        int uncompressedSize = decompressAllChunks(
        //                compressed,
        //                compressedOffset,
        //                compressedSize,
        //                uncompressed,
        //                0);
        //
        //        if (!(expectedLength == uncompressedSize)) {
        //            throw new CorruptionException(String.format("Recorded length is %s bytes but actual length after decompression is %s bytes ",
        //                    expectedLength,
        //                    uncompressedSize));
        //        }
        //
        //        return uncompressed;
        throw new UnsupportedOperationException("not yet implemented");
    }

    public static int uncompress(UnsafeSlice compressed, int compressedOffset, int compressedSize, UnsafeSlice uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        synchronized (compressed) {
            synchronized (uncompressed) {
                long inputAddress = compressed.getAddress() + compressedOffset;
                int inputLength = compressedSize;
                long outputAddress = uncompressed.getAddress() + uncompressedOffset;
                int outputLength = uncompressed.length() - uncompressedOffset;
                return uncompress(inputAddress, inputLength, outputAddress, outputLength);
            }
        }
    }

    public static int uncompress(long inputAddress, int inputLength, long outputAddress, int outputLength)
            throws CorruptionException
    {
        UnsafeMemory.inputAddress = inputAddress;
        UnsafeMemory.inputLength = inputLength;
        UnsafeMemory.outputAddress = outputAddress;
        UnsafeMemory.outputLength = outputLength;

        long inputLimit = inputAddress + inputLength;
        long outputLimit = outputAddress + outputLength;

        long[] varInt = readUncompressedLength(inputAddress);
        int expectedLength = (int) varInt[0];
        long input = varInt[1];

        SnappyInternalUtils.checkArgument(expectedLength <= outputLength,
                "Uncompressed buffer must be at least %s bytes, but is only %s bytes", expectedLength, outputLength);


        long endPosition = decompressAllChunks(
                input,
                inputLimit,
                outputAddress,
                outputLimit);

        long actualLength = endPosition - outputAddress;

        if (expectedLength != actualLength) {
            throw new CorruptionException(String.format("Recorded length is %s bytes but actual length after decompression is %s bytes ",
                    expectedLength,
                    actualLength));
        }

        return expectedLength;
    }

    private static long decompressAllChunks(
            long inputAddress,
            final long inputLimit,
            long outputAddress,
            long outputLimit)
    {
        while (inputAddress < inputLimit) {
            int chunkSize = readInt(inputAddress);
            inputAddress += 4;

            outputAddress = decompressChunk(inputAddress, inputAddress + chunkSize, outputAddress, outputLimit);
            inputAddress += chunkSize;
        }

        return outputAddress;
    }

    public static long decompressChunk(
            long input,
            final long inputLimit,
            long output,
            final long outputLimit)
            throws CorruptionException
    {
        long fastInputLimit = inputLimit - 8; // maximum offset in input buffer from which it's safe to read long-at-a-time
        long fastOutputLimit = outputLimit - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time

        while (input < inputLimit) {
            int token = readByte(input++);

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xf) {
                while (readByte(input) == 255) {
                    literalLength += 255;
                    input++;
                }
                literalLength += readByte(input++);
            }

            // copy literal
            long literalOutputLimit = output + literalLength;
            if (literalOutputLimit > fastOutputLimit || input + literalLength > fastInputLimit) {
                // slow, precise copy
                UnsafeMemory.copyMemory(input, output, literalLength);
                return literalOutputLimit;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            do {
                UnsafeMemory.copyLong(output, input);
                input += 8;
                output += 8;
            }
            while (output < literalOutputLimit);
            input -= (output - literalOutputLimit); // adjust index if we overcopied
            output = literalOutputLimit;

            // get offset
            int offset = readShort(input);
            input += 2;

            // get matchlength
            int matchLength = token & 0xf; // bottom-most 4 bits of token
            if (matchLength == 0xf) {
                while (readByte(input) == 255) {
                    matchLength += 255;
                    input++;
                }
                matchLength += readByte(input++);
            }
            matchLength += 4; // implicit length from initial 4-byte match in encoder

            // copy repeated sequence
            long source = output - offset;
            if (offset < 8) {
                // copies 8 bytes from sourceIndex to destIndex and leaves the pointers more than
                // 8 bytes apart so that we can copy long-at-a-time below
                int dec1 = DEC_TABLE_1[offset];

                copyByte(output++, source++);
                copyByte(output++, source++);
                copyByte(output++, source++);
                copyByte(output++, source++);
                source -= dec1;

                UnsafeMemory.copyInt(output, source);
                output += 4;
                source -= DEC_TABLE_2[offset];
            }
            else {
                UnsafeMemory.copyLong(output, source);
                source += 8;
                output += 8;
            }

            // subtract 8 for the bytes we copied above
            long matchOutputLimit = output + matchLength - 8;

            if (matchOutputLimit > fastOutputLimit) {
                while (output < fastOutputLimit) {
                    UnsafeMemory.copyLong(output, source);
                    source += 8;
                    output += 8;
                }

                while (output < matchOutputLimit) {
                    copyByte(output++, source++);
                }

                if (output == outputLimit) { // Check EOF (should never happen, since last 5 bytes are supposed to be literals)
                    return output;
                }
            }

            while (output < matchOutputLimit) {
                UnsafeMemory.copyLong(output, source);
                source += 8;
                output += 8;
            }

            output = matchOutputLimit; // correction in case we overcopied
        }

        return output;
    }

    /**
     * Reads the variable length integer encoded a the specified offset, and returns this length with the number of bytes read.
     */
    private static long[] readUncompressedLength(long compressed)
            throws CorruptionException
    {
        int result;
        {
            int b = readByte(compressed++);
            result = b & 0x7f;
            if ((b & 0x80) != 0) {
                b = readByte(compressed++);
                result |= (b & 0x7f) << 7;
                if ((b & 0x80) != 0) {
                    b = readByte(compressed++);
                    result |= (b & 0x7f) << 14;
                    if ((b & 0x80) != 0) {
                        b = readByte(compressed++);
                        result |= (b & 0x7f) << 21;
                        if ((b & 0x80) != 0) {
                            b = readByte(compressed++);
                            result |= (b & 0x7f) << 28;
                            if ((b & 0x80) != 0) {
                                throw new CorruptionException("last byte of compressed length int has high bit set");
                            }
                        }
                    }
                }
            }
        }
        return new long[] { result, compressed };
    }

}
