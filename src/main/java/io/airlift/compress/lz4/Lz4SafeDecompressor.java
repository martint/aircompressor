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
    private final static int[] DEC_TABLE_1 = {0, 3, 2, 3, 0, 0, 0, 0};
    private final static int[] DEC_TABLE_2 = {0, 0, 0, -1, 0, 1, 2, 3};

    private final static int MIN_MATCH = 4;
    private final static int LAST_LITERAL_SIZE = 5;

    private final static int SIZE_OF_SHORT = 2;
    private final static int SIZE_OF_INT = 4;
    private final static int SIZE_OF_LONG = 8;

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

// this seems to be slower:
//                int value;
//                do {
//                    value = readByte(input++) & 0xFF;
//                    literalLength += value;
//                }
//                while (value == 255);

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
                    return (int) -(input - inputAddress) - 1;
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

            if (output - offset < outputAddress) {
                return (int) -(input - inputAddress) - 1;
            }

            // get matchlength
            int matchLength = token & 0xF; // bottom-most 4 bits of token
            if (matchLength == 0xF) {
// this seems to be slower:
//                int value;
//                do {
//                    value = readByte(input++) & 0xFF;
//                    matchLength += value;
//                }
//                while (value == 255);
//
                while (input < inputLimit - (LAST_LITERAL_SIZE + 1)) { // Ensure enough bytes remain for LASTLITERALS + token
                    int value = UNSAFE.getByte(inputBase, input++) & 0xFF;
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
            matchLength += MIN_MATCH; // implicit length from initial 4-byte match in encoder

            // copy repeated sequence
            long matchAddress = output - offset;
            if (offset < SIZE_OF_LONG) {
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

                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                matchAddress -= dec1;

                UNSAFE.putInt(outputBase, output, UNSAFE.getInt(outputBase, matchAddress));
                output += SIZE_OF_INT;

                matchAddress -= dec2;
            }
            else {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            // subtract SIZE_OF_LONG for the bytes we copied above
            long matchOutputLimit = output + matchLength - SIZE_OF_LONG;

            if (matchOutputLimit > fastOutputLimit - MIN_MATCH) {
                if (matchOutputLimit > outputLimit - LAST_LITERAL_SIZE) {
                    return (int) -(input - inputAddress) - 1;
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
