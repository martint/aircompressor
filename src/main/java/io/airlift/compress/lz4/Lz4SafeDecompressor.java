/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.lz4;

import io.airlift.compress.CorruptionException;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;

public class Lz4SafeDecompressor
{
    private final static int SIZE_OF_SHORT = 2;
    private final static int SIZE_OF_INT = 4;
    private final static int SIZE_OF_LONG = 8;

    private final static int[] DEC_32_TABLE = {0, 3, 2, 3, 0, 0, 0, 0};
    private final static int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private final static int MIN_MATCH = 4;
    private final static int LAST_LITERAL_SIZE = 5;


    public int uncompress(Slice compressed, int compressedOffset, int compressedSize, Slice uncompressed, int uncompressedOffset)
    {
        synchronized (compressed) {
            synchronized (uncompressed) {
                return uncompress(compressed.getBase(),
                        compressed.getAddress() + compressedOffset,
                        compressedSize,
                        uncompressed.getBase(),
                        uncompressed.getAddress() + uncompressedOffset,
                        uncompressed.length() - uncompressedOffset);
            }
        }
    }

    private int uncompress(
            final Object inputBase,
            final long inputAddress,
            final int inputLength,
            final Object outputBase,
            final long outputAddress,
            final int outputLength)
            throws CorruptionException
    {
        final long inputLimit = inputAddress + inputLength;
        final long outputLimit = outputAddress + outputLength;
        final long fastOutputLimit = outputLimit - SizeOf.SIZE_OF_LONG; // maximum offset in output buffer to which it's safe to write long-at-a-time

        long input = inputAddress;
        long output = outputAddress;

        while (true) {
            final int token = UNSAFE.getByte(inputBase, input++) & 0xFF;

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xF) {

                int value = 255;
                while (input < inputLimit && value == 255) {
                    value = UNSAFE.getByte(inputBase, input++) & 0xFF;
                    literalLength += value;
                }
            }

            // copy literal
            long literalOutputLimit = output + literalLength;
            if (literalOutputLimit > (fastOutputLimit - MIN_MATCH) || input + literalLength > inputLimit - (2 + 1 + LAST_LITERAL_SIZE)) {
                if (literalOutputLimit > outputLimit || input + literalLength != inputLimit) {
                    throw new MalformedInputException((int) (input - inputAddress));
                }

                // slow, precise copy
                UNSAFE.copyMemory(inputBase, input, outputBase, output, literalLength);
                input += literalLength;
                output += literalLength;
                break;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
                input += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }
            while (output < literalOutputLimit);
            input -= (output - literalOutputLimit); // adjust index if we overcopied
            output = literalOutputLimit;

            // get offset
            int offset = UNSAFE.getShort(inputBase, input) & 0xFFFF;
            input += SIZE_OF_SHORT;

            long matchAddress = output - offset;
            if (matchAddress < outputAddress) {
                throw new MalformedInputException((int) (input - inputAddress));
            }

            // get matchlength
            int matchLength = token & 0xF; // bottom-most 4 bits of token
            if (matchLength == 0xF) {
                while (input < inputLimit - (LAST_LITERAL_SIZE + 1)) { // Ensure enough bytes remain for LASTLITERALS + token
                    int value = UNSAFE.getByte(inputBase, input++) & 0xFF;
                    matchLength += value;
                    if (value == 255) {
                        continue;
                    }
                    break;
                }
            }
            matchLength += MIN_MATCH; // implicit length from initial 4-byte match in encoder

            long matchOutputLimit = output + matchLength;

            // copy repeated sequence
            if (offset < SIZE_OF_LONG) {
                // 8 bytes apart so that we can copy long-at-a-time below
                int decrement32 = DEC_32_TABLE[offset];
                int decrement64 = DEC_64_TABLE[offset];

                UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
                UNSAFE.putByte(outputBase, output + 1, UNSAFE.getByte(outputBase, matchAddress + 1));
                UNSAFE.putByte(outputBase, output + 2, UNSAFE.getByte(outputBase, matchAddress + 2));
                UNSAFE.putByte(outputBase, output + 3, UNSAFE.getByte(outputBase, matchAddress + 3));
                matchAddress += SIZE_OF_INT;
                matchAddress -= decrement32;
                output += SIZE_OF_INT;

                UNSAFE.putInt(outputBase, output, UNSAFE.getInt(outputBase, matchAddress));
                output += SIZE_OF_INT;
                matchAddress -= decrement64;
            }
            else {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }


            if (matchOutputLimit > fastOutputLimit - MIN_MATCH) {
                if (matchOutputLimit > outputLimit - LAST_LITERAL_SIZE) {
                    throw new MalformedInputException((int) (input - inputAddress));
                }

                if (output < fastOutputLimit) {
                    do {
                        UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                        matchAddress += SIZE_OF_LONG;
                        output += SIZE_OF_LONG;
                    }
                    while (output < fastOutputLimit);
                }

                while (output < matchOutputLimit) {
                    UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                }
                output = matchOutputLimit; // correction in case we overcopied
                continue;
            }

            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }
            while (output < matchOutputLimit);

            output = matchOutputLimit; // correction in case we overcopied
        }

        return (int) (output - outputAddress);
    }
}
