package io.airlift.compress;

import io.airlift.compress.slice.UnsafeSlice;

import static io.airlift.compress.UnsafeMemory.copyByte;
import static io.airlift.compress.UnsafeMemory.outputAddress;
import static io.airlift.compress.UnsafeMemory.readByte;
import static io.airlift.compress.UnsafeMemory.readInt;
import static io.airlift.compress.UnsafeMemory.readShort;

public class Lz4DirectMemoryDecompressor
{
    private final static int[] DEC_TABLE_1 = { 0, 3, 2, 3, 0, 0, 0, 0 };
    private final static int[] DEC_TABLE_2 = { 0, 0, 0, -1, 0, 1, 2, 3 };

//    public static int getUncompressedLength(UnsafeSlice compressed, int compressedOffset)
//            throws CorruptionException
//    {
//        return (int) readUncompressedLength(compressed.getAddress() + compressedOffset)[0];
//    }

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

        int expectedLength = readInt(inputAddress);
        long input = inputAddress + 4;

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
            long outputOffset,
            final long outputLimit)
            throws CorruptionException
    {
        long output = outputOffset;
        long fastInputLimit = inputLimit - 8; // maximum offset in input buffer from which it's safe to read long-at-a-time
        long fastOutputLimit = outputLimit - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time

        while (true) {
            int token = readByte(input++);

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xf) {

// this seems to be slower:
//                int value;
//                do {
//                    value = readByte(input++) & 0xFF;
//                    literalLength += value;
//                }
//                while (value == 255);

                int value = 255;
                while (input < inputLimit && value == 255) {
                    value = readByte(input++);
                    literalLength += value;
                }
            }

            // copy literal
            long literalOutputLimit = output + literalLength;
            if (literalOutputLimit > (fastOutputLimit + 4) || input + literalLength > inputLimit - (2+1+5)) {
                if (literalOutputLimit > outputLimit || input + literalLength != inputLimit) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                // slow, precise copy
                UnsafeMemory.copyMemory(input, output, literalLength);
                input += literalLength;
                output += literalLength;
                break;
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
            int offset = readShort(input) & 0xFFFF;
            input += 2;

            if (output - offset < outputOffset) {
                throw new ArrayIndexOutOfBoundsException();
            }

            // get matchlength
            int matchLength = token & 0xf; // bottom-most 4 bits of token
            if (matchLength == 0xf) {
// this seems to be slower:
//                int value;
//                do {
//                    value = readByte(input++) & 0xFF;
//                    matchLength += value;
//                }
//                while (value == 255);
//
                while (input < inputLimit - (5 + 1)) {
                    int value = readByte(input++);
                    matchLength += value;
                    if (value == 255) {
                        continue;
                    }
                    break;
                }
//                while (readByte(input) == 255) {
//                    matchLength += 255;
//                    input++;
//                }
//                matchLength += readByte(input++);
            }
//            matchLength += 4; // implicit length from initial 4-byte match in encoder

            // copy repeated sequence
            long source = output - offset;
            if (offset < 8) {
                // copies 8 bytes from sourceIndex to destIndex and leaves the pointers more than
                // 8 bytes apart so that we can copy long-at-a-time below
                int dec1 = DEC_TABLE_1[offset];
                int dec2 = DEC_TABLE_2[offset];

// this seems to be slower:
//                int dec1 = 0;
//                int dec2 = 0;
//                switch (offset) {
//                    case 1:
//                        dec1 = 3;
//                        break;
//                    case 2:
//                        dec1 = 2;
//                        break;
//                    case 3:
//                        dec1 = 3;
//                        dec2 = -1;
//                        break;
//                    case 5:
//                        dec2 = 1;
//                        break;
//                    case 6:
//                        dec2 = 2;
//                        break;
//                    case 7:
//                        dec2 = 3;
//                        break;
//                }

                copyByte(output++, source++);
                copyByte(output++, source++);
                copyByte(output++, source++);
                copyByte(output++, source++);
                source -= dec1;

                UnsafeMemory.copyInt(output, source);
                output += 4;

                source -= dec2;
            }
            else {
                UnsafeMemory.copyLong(output, source);
                source += 8;
                output += 8;
            }

            // subtract 8 for the bytes we copied above
            long matchOutputLimit = output + matchLength - (8 - 4);

            if (matchOutputLimit > fastOutputLimit - 4) {
                if (matchOutputLimit > outputLimit - 5) {
                    throw new ArrayIndexOutOfBoundsException();
                }

                if (output < fastOutputLimit) {
                    do {
                        UnsafeMemory.copyLong(output, source);
                        source += 8;
                        output += 8;
                    }
                    while (output < fastOutputLimit);
                }

                while (output < matchOutputLimit) {
                    copyByte(output++, source++);
                }
                output = matchOutputLimit; // correction in case we overcopied
                continue;
            }

            do {
                UnsafeMemory.copyLong(output, source);
                source += 8;
                output += 8;
            }
            while (output < matchOutputLimit);

            output = matchOutputLimit; // correction in case we overcopied
        }

        return output;
    }

    /**
     * Reads the variable length integer encoded a the specified offset, and returns this length with the number of bytes read.
     */
//    private static long[] readUncompressedLength(long compressed)
//            throws CorruptionException
//    {
//        int result;
//        {
//            int b = readByte(compressed++);
//            result = b & 0x7f;
//            if ((b & 0x80) != 0) {
//                b = readByte(compressed++);
//                result |= (b & 0x7f) << 7;
//                if ((b & 0x80) != 0) {
//                    b = readByte(compressed++);
//                    result |= (b & 0x7f) << 14;
//                    if ((b & 0x80) != 0) {
//                        b = readByte(compressed++);
//                        result |= (b & 0x7f) << 21;
//                        if ((b & 0x80) != 0) {
//                            b = readByte(compressed++);
//                            result |= (b & 0x7f) << 28;
//                            if ((b & 0x80) != 0) {
//                                throw new CorruptionException("last byte of compressed length int has high bit set");
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        return new long[] { result, compressed };
//    }

}
