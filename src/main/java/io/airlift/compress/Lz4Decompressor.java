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
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

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

        while (inputIndex < input.address + inputOffset + inputSize) {
            int chunkSize = Slice.unsafe.getInt(input.base, inputIndex); //input.getInt(inputIndex);
            inputIndex += 4;

            outputIndex += decompressChunk(input, inputIndex, chunkSize, output, outputIndex);
            inputIndex += chunkSize;
        }

        return (int) (outputIndex - outputOffset - output.address);
    }

    public static int decompressChunk(
            final Slice input,
            final long inputBase,
            final int inputSize,
            final Slice output,
            final long outputBase)
            throws CorruptionException
    {
        long inputIndex = inputBase; //input.address + inputOffset;
        long outputIndex = outputBase;

        long inputLimit = inputIndex + inputSize;
        long fastInputLimit = inputLimit - 8; // maximum offset in input buffer from which it's safe to read long-at-a-time
        long fastOutputLimit = output.address + output.length() - 8; // maximum offset in output buffer to which it's safe to write long-at-a-time

        while (inputIndex < inputLimit) {
            int token = Slice.unsafe.getByte(input.base, inputIndex++) & 0xFF;

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xf) {
                while ((Slice.unsafe.getByte(input.base, inputIndex)) == -1) {
                    literalLength += 255;
                    inputIndex++;
                }
                literalLength += (Slice.unsafe.getByte(input.base, inputIndex++) & 0xFF);
            }

            // copy literal
            long literalOutputLimit = outputIndex + literalLength;
            if (literalOutputLimit > fastOutputLimit || inputIndex + literalLength > fastInputLimit) {
                // slow, precise copy
//                Slice.unsafe.copyMemory(input.base, inputIndex, output.base, outputIndex, literalLength);
                copy(output, outputIndex, input, inputIndex, literalLength);
                return (int) (literalOutputLimit - outputBase); //outputIndex + literalLength;
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
            int offset = Slice.unsafe.getShort(input.base, inputIndex) & 0xFFFF;
            inputIndex += 2;

            // get matchlength
            int matchLength = token & 0xf; // bottom-most 4 bits of token
            if (matchLength == 0xf) {
                while ((Slice.unsafe.getByte(input.base, inputIndex)) == -1) {
                    matchLength += 255;
                    inputIndex++;
                }
                matchLength += (Slice.unsafe.getByte(input.base, inputIndex++) & 0xFF);

//                byte value = Slice.unsafe.getByte(input.base, inputIndex++);
//                while (value == -1) {
//                    matchLength += 255;
//                    value = Slice.unsafe.getByte(input.base, inputIndex++);
//                }
//                matchLength += (value & 0xFF);

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

            long minOutputLimit = Math.min(matchOutputLimit, fastOutputLimit);
            while (outputIndex < minOutputLimit) {
                Slice.unsafe.putLong(output.base, outputIndex, Slice.unsafe.getLong(output.base, sourceIndex));
                sourceIndex += 8;
                outputIndex += 8;
            }

            while (outputIndex < matchOutputLimit) {
                Slice.unsafe.putByte(output.base, outputIndex++, Slice.unsafe.getByte(output.base, sourceIndex++));
            }

            if (outputIndex == output.length()) { // Check EOF (should never happen, since last 5 bytes are supposed to be literals)
                return (int) (outputIndex - outputBase);
            }

            outputIndex = matchOutputLimit; // correction in case we overcopied
        }

        return (int) (outputIndex - outputBase);
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
