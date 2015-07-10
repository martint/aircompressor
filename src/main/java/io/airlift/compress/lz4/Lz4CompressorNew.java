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

import io.airlift.slice.Slice;

import java.util.Arrays;

import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;

public class Lz4CompressorNew
{
    private final static ControlException EXCEPTION = new ControlException();

    private final static int SIZE_OF_SHORT = 2;
    private final static int SIZE_OF_INT = 4;
    private final static int SIZE_OF_LONG = 8;

    private static final int MAX_INPUT_SIZE = 0x7E000000;   /* 2 113 929 216 bytes */

    private final static int MIN_MATCH = 4;

    private static final int MEMORY_USAGE = 14;
    private static final int STREAM_SIZE = 2 * ((1 << (MEMORY_USAGE - 3)) + 4);
    private static final int[] buffer = new int[STREAM_SIZE];

    private static final int HASHLOG = MEMORY_USAGE - 2;

    private static final int HASH_SHIFT = ((MIN_MATCH * 8) - HASHLOG);

    private final static int COPY_LENGTH = 8;
    private final static int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;
    private final static int LAST_LITERALS = 5;

    private final static int MIN_LENGTH = MATCH_FIND_LIMIT + 1;

    private final static int ML_BITS = 4;
    private final static int ML_MASK = (1 << ML_BITS) - 1;
    private final static int RUN_BITS = 8 - ML_BITS;
    private final static int RUN_MASK = (1 << RUN_BITS) - 1;

    private final static int MAX_DISTANCE = ((1 << 16) - 1);

    private static final int SKIP_TRIGGER = 6;  /* Increase this value ==> compression run slower on incompressible data */

    private static int hash(int value)
    {
        return ((int) (value * 2654435761L)) >>> HASH_SHIFT;
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return sourceLength + sourceLength / 255 + 16;
    }

    public int compress(Slice uncompressed, int uncompressedOffset, int uncompressedSize, Slice compressed, int compressedOffset)
    {
        Arrays.fill(buffer, 0);
        return compress(uncompressed.getBase(),
                uncompressed.getAddress() + uncompressedOffset,
                uncompressedSize,
                compressed.getBase(),
                compressed.getAddress() + compressedOffset,
                buffer);
    }

    private static int compress(
            final Object inputBase,
            final long inputAddress,
            final int inputLength,
            final Object outputBase,
            final long outputAddress,
            final int[] table)
    {
        if (inputLength > MAX_INPUT_SIZE) {
            throw new IllegalArgumentException("Max input length exceeded");
        }

        long input = inputAddress;
        long output = outputAddress;

        final long inputLimit = inputAddress + inputLength;
        final long matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
        final long matchLimit = inputLimit - LAST_LITERALS;

        if (inputLength < MIN_LENGTH) {
            output = emitLastLiteral(outputBase, output, inputBase, input, inputLimit - input);
            return (int) (output - outputAddress);
        }

        long anchor = input;

        // First Byte
        // put position in hash
        table[hash(UNSAFE.getInt(inputBase, input))] = (int) (input - inputAddress);

        input++;
        int nextHash = hash(UNSAFE.getInt(inputBase, input));

        boolean done = false;
        do {
            long nextInputIndex = input;
            int findMatchAttempts = 1 << SKIP_TRIGGER;
            int step = 1;

            // find 4-byte match
            long matchIndex;
            do {
                int hash = nextHash;
                input = nextInputIndex;
                nextInputIndex += step;

                step = (findMatchAttempts++) >>> SKIP_TRIGGER;

                if (nextInputIndex > matchFindLimit) {
                    return (int) (emitLastLiteral(outputBase, output, inputBase, anchor, inputLimit - anchor) - outputAddress);
                }

                // get position on hash
                matchIndex = inputAddress + table[hash];
                nextHash = hash(UNSAFE.getInt(inputBase, nextInputIndex));

                // put position on hash
                table[hash] = (int) (input - inputAddress);
            }
            while (UNSAFE.getInt(inputBase, matchIndex) != UNSAFE.getInt(inputBase, input) || matchIndex + MAX_DISTANCE < input);

            // catch up
            while (input > anchor && (matchIndex > inputAddress) && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, matchIndex - 1)) {
                --input;
                --matchIndex;
            }

            int length = (int) (input - anchor);
            long tokenAddress = output++;

            output = encodeRunLength(outputBase, tokenAddress, output, length);

            // emitLiteral copies in chunks of 8 bytes. We are guaranteed to have at least 8 bytes available in output (how?)
            output = emitLiteral(outputBase, output, inputBase, anchor, length);

            // next match
            while (true) {
                output = encodeOffset(outputBase, output, (short) (input - matchIndex));

                // find match length
                input += MIN_MATCH;
                int matchLength = count(inputBase, input, matchIndex + MIN_MATCH, matchLimit);
                input += matchLength;

                // TODO
//                if ((outputLimited) && (unlikely(op + (1 + LASTLITERALS) + (matchLength>>8) > olimit))) {
//                    return 0;    // Check output limit
//                }

                // Encode MatchLength
                output = encodeMatchLength(outputBase, tokenAddress, output, matchLength);
                anchor = input;

                // are we done?
                if (input > matchFindLimit) {
                    done = true;
                    break;
                }

                long position = input - 2;
                table[hash(UNSAFE.getInt(inputBase, position))] = (int) (position - inputAddress);

                // Test next position
                int hash = hash(UNSAFE.getInt(inputBase, input));
                matchIndex = inputAddress + table[hash];
                table[hash] = (int) (input - inputAddress);

                if (matchIndex + MAX_DISTANCE < input || UNSAFE.getInt(inputBase, matchIndex) != UNSAFE.getInt(inputBase, input)) {
                    input++;
                    nextHash = hash(UNSAFE.getInt(inputBase, input));
                    break;
                }

                // go for another match
                tokenAddress = output++;
                UNSAFE.putByte(outputBase, tokenAddress, (byte) 0);
            }
        }
        while (!done);

        // Encode Last Literals
        output = emitLastLiteral(output, output, input, anchor, inputLimit - anchor);

        return (int) (output - outputAddress);
    }

    private static int count(Object inputBase, final long start, long matchStart, long matchLimit)
    {
        long current = start;

        try {
            // first, compare long at a time
            while (current < matchLimit - (SIZE_OF_LONG - 1)) {
                long diff = UNSAFE.getLong(inputBase, matchStart) ^ UNSAFE.getLong(inputBase, current);
                if (diff != 0) {
                    current += Long.numberOfTrailingZeros(diff) >> 3;
                    throw EXCEPTION;
//                    return (int) (current - start);
                }

                current += SIZE_OF_LONG;
                matchStart += SIZE_OF_LONG;
            }

            if (current < matchLimit - (SIZE_OF_INT - 1) && UNSAFE.getInt(inputBase, matchStart) == UNSAFE.getInt(inputBase, current)) {
                current += SIZE_OF_INT;
                matchStart += SIZE_OF_INT;
            }

            if (current < matchLimit - (SIZE_OF_SHORT - 1) && UNSAFE.getShort(inputBase, matchStart) == UNSAFE.getShort(inputBase, current)) {
                current += SIZE_OF_SHORT;
                matchStart += SIZE_OF_SHORT;
            }

            if (current < matchLimit && UNSAFE.getByte(inputBase, matchStart) == UNSAFE.getByte(inputBase, current)) {
                ++current;
            }
        }
        catch (ControlException e) {
        }

        return (int) (current - start);
    }

    private static long emitLastLiteral(
            final Object outputBase,
            final long outputAddress,
            final Object inputBase,
            final long inputAddress,
            final long length)
    {
        long output = encodeRunLength(outputBase, outputAddress, outputAddress + 1, length);
        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, output, length);

        return output + length;
    }

    private static long encodeRunLength(
            final Object base,
            final long tokenAddress,
            final long continuationAddress,
            final long length)
    {
//        if ((outputLimited) && (unlikely(op + litLength + (2 + 1 + LASTLITERALS) + (litLength/255) > olimit))) {
//            return 0;   *//* Check output limit *//*
//        }

        long output = continuationAddress;
        if (length >= RUN_MASK) {
            UNSAFE.putByte(base, tokenAddress, (byte) (RUN_MASK << ML_BITS));
            long remaining = length - RUN_MASK;
            while (remaining > 254) {
                UNSAFE.putByte(base, output++, (byte) 255);
                remaining -= 255;
            }
            UNSAFE.putByte(base, output++, (byte) remaining);
        }
        else {
            UNSAFE.putByte(base, tokenAddress, (byte) (length << ML_BITS));
        }

        return output;
    }

    private static long emitLiteral(
            final Object outputBase,
            final long outputAddress,
            final Object inputBase,
            final long inputAddress,
            final long length)
    {
        final long outputLimit = outputAddress + length;

        long output = outputAddress;
        long input = inputAddress;
        do {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
        }
        while (output < outputLimit);

        return outputLimit;
    }

    private static long encodeOffset(final Object outputBase, final long outputAddress, final short offset)
    {
        UNSAFE.putShort(outputBase, outputAddress, offset);
        return outputAddress + 2;
    }

    private static long encodeMatchLength(
            final Object outputBase,
            final long tokenAddress,
            final long continuationAddress,
            final long length)
    {
        long output = continuationAddress;
        if (length >= ML_MASK) {
            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) + ML_MASK));
            long remaining = length - ML_MASK;
            while (remaining > 509) {
                UNSAFE.putShort(outputBase, output, (short) 0xFFFF);
                output += SIZE_OF_SHORT;
                remaining -= 510;
            }
            if (remaining > 254) {
                UNSAFE.putByte(outputBase, output++, (byte) 255);
                remaining -= 255;
            }
            UNSAFE.putByte(outputBase, output++, (byte) remaining);
        }
        else {
            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) + length));
        }

        return output;
    }

    private final static class ControlException
            extends Exception {}

    /*
static void LZ4_putPosition(const BYTE* p, void* tableBase, tableType_t tableType, const BYTE* srcBase)
{
    U32 h = LZ4_hashPosition(p, tableType);
    LZ4_putPositionOnHash(p, h, tableBase, tableType, srcBase);
}

static U32 LZ4_hashPosition(const BYTE* p, tableType_t tableType)
{
    return (((LZ4_read32(p)) * 2654435761U) >> ((MINMATCH*8)-LZ4_HASHLOG));
}

static void LZ4_putPositionOnHash(const BYTE* p, U32 h, void* tableBase, tableType_t tableType, const BYTE* srcBase)
{
    U32* hashTable = (U32*) tableBase;
    hashTable[h] = (U32)(p-srcBase);
}

static U32 LZ4_hashSequence(U32 sequence)
{

}

     */
}
