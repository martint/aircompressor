package io.airlift.compress;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class Lz4Decompressor
{
    private final static int[] DEC_TABLE_1 = { 0, 3, 2, 3, 0, 0, 0, 0 };
    private final static int[] DEC_TABLE_2 = { 0, 0, 0, -1, 0, 1, 2, 3 };

    public static int getUncompressedLength(Slice compressed, int compressedOffset)
            throws CorruptionException
    {
        return readUncompressedLength(compressed, compressedOffset)[0];
    }

    public static Slice uncompress(Slice compressed, int compressedOffset, int compressedSize)
            throws CorruptionException
    {
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

        // allocate the uncompressed buffer
        Slice uncompressed = Slices.allocate(expectedLength);

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

    public static int uncompress(Slice compressed, int compressedOffset, int compressedSize, Slice uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        int expectedLength = Slice.unsafe.getInt(compressed.base, compressed.address + compressedOffset);
        compressedOffset += 4;

        SnappyInternalUtils.checkArgument(expectedLength <= uncompressed.length() - uncompressedOffset,
                "Uncompressed length %s must be less than %s", expectedLength, uncompressed.length() - uncompressedOffset);

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
            final Slice input,
            final int inputOffset,
            final int inputSize,
            final Slice output,
            final int outputOffset)
    {
        long outputIndex = output.address + outputOffset;
        long inputIndex = input.address + inputOffset;
        long inputLimit = input.address + inputOffset + inputSize;

        while (inputIndex < inputLimit) {
            int outputSize = Slice.unsafe.getInt(input.base, inputIndex);
            inputIndex += 4;

            inputIndex += decompressChunk(input, inputIndex, output, outputIndex, outputSize);
            outputIndex += outputSize;
        }

        return (int) (outputIndex - outputOffset - output.address);
    }

    public static int decompressChunk(
            final Slice input,
            final long inputOffset,
            final Slice output,
            final long outputOffset,
            final int outputSize)
            throws CorruptionException
    {
        long inputIndex = inputOffset;
        long outputIndex = outputOffset;

        long outputLimit = outputOffset + outputSize;
        long fastOutputLimit = outputLimit - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time

        while (true) {
            int token = Slice.unsafe.getByte(input.base, inputIndex++) & 0xFF;

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xf) {
                int value;
                do {
                    value = (Slice.unsafe.getByte(input.base, inputIndex++)) & 0xFF;
                    literalLength += value;
                }
                while (value == 255);
            }

            // copy literal
            long literalOutputLimit = outputIndex + literalLength;
            if (literalOutputLimit > fastOutputLimit) {
                if (literalOutputLimit != outputLimit) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                // slow, precise copy
                Slice.unsafe.copyMemory(input.base, inputIndex, output.base, outputIndex, literalLength);
//                copy(output, outputIndex, input, inputIndex, literalLength);
                outputIndex += literalLength;
                inputIndex += literalLength;
                break;
//                return (int) (literalOutputLimit - outputOffset); //outputIndex + literalLength;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            do {
                Slice.unsafe.putLong(output.base, outputIndex, Slice.unsafe.getLong(input.base, inputIndex));
                inputIndex += 8;
                outputIndex += 8;
            }
            while (outputIndex < literalOutputLimit);
            inputIndex -= (outputIndex - literalOutputLimit); // adjust index if we overcopied
            outputIndex = literalOutputLimit;

            // get offset
            int offset = Slice.unsafe.getShort(input.base, inputIndex);// & 0xFFFF;
            inputIndex += 2;

            if (outputIndex - offset < outputOffset) {
                throw new ArrayIndexOutOfBoundsException();
            }

            // get matchlength
            int matchLength = token & 0xf; // bottom-most 4 bits of token
            if (matchLength == 0xf) {
                int value;
                do {
                    value = (Slice.unsafe.getByte(input.base, inputIndex++)) & 0xFF;
                    matchLength += value;
                }
                while (value == 255);
            }
            matchLength += 4; // implicit length from initial 4-byte match in encoder

            // copy repeated sequence
            long sourceIndex = outputIndex - offset;
            if (offset < 8) {
                // copies 8 bytes from sourceIndex to destIndex and leaves the pointers more than
                // 8 bytes apart so that we can copy long-at-a-time below

                Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
                Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
                Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
                Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
                sourceIndex -= DEC_TABLE_1[offset];

                Slice.unsafe.putInt(output.base, outputIndex, Slice.unsafe.getInt(output.base, sourceIndex));
                outputIndex += 4;
                sourceIndex -= DEC_TABLE_2[offset];
            }
            else {
                Slice.unsafe.putLong(output.base, outputIndex, Slice.unsafe.getLong(output.base, sourceIndex));
                outputIndex += 8;
                sourceIndex += 8;
            }

            // subtract 8 for the bytes we copied above
            long matchOutputLimit = outputIndex + matchLength - 8;

            if (matchOutputLimit > fastOutputLimit) {
                if (matchOutputLimit > outputLimit - 5) {
                    throw new ArrayIndexOutOfBoundsException();
                }

                // match output limit is beyond the max safe limit for copying long-at-a-time
                // so first copy up to fastOutputLimit
                if (outputIndex < fastOutputLimit) {
                    do {
                        Slice.unsafe.putLong(output.base, outputIndex, Slice.unsafe.getLong(output.base, sourceIndex));
                        sourceIndex += 8;
                        outputIndex += 8;
                    }
                    while (outputIndex < fastOutputLimit);
                }

                // and then do byte-at-a-time until matchOutputLimit
                while (outputIndex < matchOutputLimit) {
                    Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
                }

//                // correction in case we overcopied (i.e., matchOutputLimit - fastOutputLimit < SIZE_OF_LONG)
//                outputIndex = matchOutputLimit;
            }
            else {
                do {
                    Slice.unsafe.putLong(output.base, outputIndex, Slice.unsafe.getLong(output.base, sourceIndex));
                    sourceIndex += 8;
                    outputIndex += 8;
                }
                while (outputIndex < matchOutputLimit);
            }

            outputIndex = matchOutputLimit; // correction in case we overcopied
        }

//        return (int) (outputIndex - outputOffset);
        return (int) (inputIndex - inputOffset);
    }

    private static void copy(Slice destination, long destinationIndex, Slice source, long offset, int length)
    {
        long sourceIndex = offset;

        while (length >= SIZE_OF_LONG) {
            long value = Slice.unsafe.getLong(source.base, sourceIndex);
            Slice.unsafe.putLong(destination.base, destinationIndex, value);

            sourceIndex += SIZE_OF_LONG;
            destinationIndex += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte value = Slice.unsafe.getByte(source.base, sourceIndex++);
            Slice.unsafe.putByte(destination.base, destinationIndex++, value);
            length--;
        }
    }

//    public static int decompressChunkFast(
//            final byte[] input,
//            final int inputOffset,
//            final byte[] output,
//            final int outputOffset,
//            final int outputSize)
//    throws CorruptionException
//    {
//        int inputIndex = inputOffset;
//        int outputIndex = outputOffset;
//
////        int inputLimit = inputOffset + inputSize;
////        int fastInputLimit = inputLimit - 8; // maximum offset in input buffer from which it's safe to read long-at-a-time
//        int outputLimit = outputOffset + outputSize;
//        int fastOutputLimit = output.length - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time
//
//        while (true) {
//            int token = loadByte(input, inputIndex++);
//
//            // decode literal length
//            int literalLength = token >>> 4; // top-most 4 bits of token
//            if (literalLength == 0xf) {
//                int v = 255;
//                while (v == 255) {
//                    v = loadByte(input, inputIndex) & 0xff;
//                    literalLength += v;
//                }
////                while (loadByte(input, inputIndex) == 255) {
////                    literalLength += 255;
////                    inputIndex++;
////                }
////                literalLength += loadByte(input, inputIndex++);
//            }
//
//            // copy literal
//            int literalOutputLimit = outputIndex + literalLength;
////            if (literalOutputLimit > fastOutputLimit || inputIndex + literalLength > fastInputLimit) {
////                // slow, precise copy
////                copyMemory(input, inputIndex, output, outputIndex, literalLength);
////                return literalOutputLimit; //outputIndex + literalLength;
////            }
//
//            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
//            do {
//                copyLong(input, inputIndex, output, outputIndex);
//                inputIndex += 8;
//                outputIndex += 8;
//            }
//            while (outputIndex < literalOutputLimit);
//            inputIndex -= (outputIndex - literalOutputLimit); // adjust index if we overcopied
//            outputIndex = literalOutputLimit;
//
//            // get offset
//            int offset = loadShort(input, inputIndex);
//            inputIndex += 2;
//
//            // get matchlength
//            int matchLength = token & 0xf; // bottom-most 4 bits of token
//            if (matchLength == 0xf) {
//                while (loadByte(input, inputIndex) == 255) {
//                    matchLength += 255;
//                    inputIndex++;
//                }
//                matchLength += loadByte(input, inputIndex++);
//            }
//            matchLength += 4; // implicit length from initial 4-byte match in encoder
//
//            // copy repeated sequence
//            int sourceIndex = outputIndex - offset;
//            if (offset < 8) {
//                // copies 8 bytes from sourceIndex to destIndex and leaves the pointers more than
//                // 8 bytes apart so that we can copy long-at-a-time below
//                int dec1 = DEC_TABLE_1[offset];
//
//                output[outputIndex++] = output[sourceIndex++];
//                output[outputIndex++] = output[sourceIndex++];
//                output[outputIndex++] = output[sourceIndex++];
//                output[outputIndex++] = output[sourceIndex++];
//                sourceIndex -= dec1;
//
//                copyInt(output, sourceIndex, output, outputIndex);
//                outputIndex += 4;
//                sourceIndex -= DEC_TABLE_2[offset];
//            }
//            else {
//                copyLong(output, sourceIndex, output, outputIndex);
//                outputIndex += 8;
//                sourceIndex += 8;
//            }
//
//            // subtract 8 for the bytes we copied above
//            int matchOutputLimit = outputIndex + matchLength - 8;
//
//            if (matchOutputLimit > fastOutputLimit) {
//                while (outputIndex < fastOutputLimit) {
//                    copyLong(output, sourceIndex, output, outputIndex);
//                    sourceIndex += 8;
//                    outputIndex += 8;
//                }
//
//                while (outputIndex < matchOutputLimit) {
//                    output[outputIndex++] = output[sourceIndex++];
//                }
//
//                if (outputIndex == output.length) { // Check EOF (should never happen, since last 5 bytes are supposed to be literals)
//                    return outputIndex;
//                }
//            }
//
//            while (outputIndex < matchOutputLimit) {
//                copyLong(output, sourceIndex, output, outputIndex);
//                sourceIndex += 8;
//                outputIndex += 8;
//            }
//
//            outputIndex = matchOutputLimit; // correction in case we overcopied
//        }
//
////        return outputIndex;
//    }

    /**
     * Reads the variable length integer encoded a the specified offset, and returns this length with the number of bytes read.
     */
    private static int[] readUncompressedLength(Slice compressed, int compressedOffset)
            throws CorruptionException
    {
        int result;
        int bytesRead = 0;
        {
            int b = compressed.getUnsignedByte(compressedOffset + bytesRead++);
            result = b & 0x7f;
            if ((b & 0x80) != 0) {
                b = compressed.getUnsignedByte(compressedOffset + bytesRead++);
                result |= (b & 0x7f) << 7;
                if ((b & 0x80) != 0) {
                    b = compressed.getUnsignedByte(compressedOffset + bytesRead++);
                    result |= (b & 0x7f) << 14;
                    if ((b & 0x80) != 0) {
                        b = compressed.getUnsignedByte(compressedOffset + bytesRead++);
                        result |= (b & 0x7f) << 21;
                        if ((b & 0x80) != 0) {
                            b = compressed.getUnsignedByte(compressedOffset + bytesRead++);
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
