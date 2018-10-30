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
package io.airlift.compress.zstd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.airlift.compress.zstd.Constants.SIZE_OF_INT;
import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class DoubleFastBlockCompressor
        implements BlockCompressor
{
    public static final int MIN_MATCH = 3;
    private static final int HASH_READ_SIZE = 8;
    private static final int SEARCH_STRENGTH = 8;
    private static final int REP_MOVE = Constants.REP_CODE_COUNT - 1;

    public int compressBlock(Object inputBase, final long inputAddress, int inputSize, SequenceStore sequenceStore, MatchState matchState, int[] rep, CompressionParameters parameters)
    {
        int matchSearchLength = Math.max(parameters.getSearchLength(), 4);

        int[] longHashTable = matchState.hashTable;
        int longHashBits = parameters.getHashLog();
        int[] shortHashTable = matchState.chainTable;
        int shortHashBits = parameters.getChainLog();

        long baseAddress = matchState.window.baseAddress;
        long input = inputAddress;
        long anchor = inputAddress;
        final long inputEnd = inputAddress + inputSize;
        final long inputLimit = inputEnd - HASH_READ_SIZE;

        int prefixLowestIndex = matchState.window.dictLimit;
        long prefixLowest = baseAddress + prefixLowestIndex;

        int offset1 = rep[0];
        int offset2 = rep[1];

        int offsetSaved = 0;

        DebugLog.print("prefixLowestIndex: %d, hBitsL: %d, hBitsS: %d, srcSize: %d, offset_1: %d, offset_2: %d, mls: %d", prefixLowestIndex, longHashBits, shortHashBits, inputSize, offset1, offset2, matchSearchLength);

        int dictAndPrefixLength = (int) (input - prefixLowest);

        /* init */
        input += (dictAndPrefixLength == 0) ? 1 : 0;
        int maxRep = (int) (input - prefixLowest);

        if (offset2 > maxRep) {
            offsetSaved = offset2;
            offset2 = 0;
        }

        if (offset1 > maxRep) {
            offsetSaved = offset1;
            offset1 = 0;
        }

        /* Main Search Loop */
        while (input < inputLimit) {   /* < instead of <=, because repcode check at (input+1) */
            int shortHash = hash(inputBase, input, shortHashBits, matchSearchLength);
            int shortMatchOffset = shortHashTable[shortHash];

            int longHash = (int) (hash8(UNSAFE.getLong(inputBase, input), longHashBits));
            int longMatchOffset = longHashTable[longHash];

            /* update hash tables */
            int current = (int) (input - baseAddress);
            longHashTable[longHash] = current;
            shortHashTable[shortHash] = current;

//            DebugLog.print("main loop: h=%d, h2=%d, current=%d, matchIndexL=%d, matchIndexS=%d", shortHash, longHash, current, longMatchOffset, shortMatchOffset);

            int matchLength;
            int offset;

            if (offset1 > 0 && UNSAFE.getInt(inputBase, input + 1 - offset1) == UNSAFE.getInt(inputBase, input + 1)) {
                // found a repeated sequence of at least 4 bytes, separated by offset1
                matchLength = count(inputBase, input + 1 + SIZE_OF_INT, input + 1 + SIZE_OF_INT - offset1, inputEnd) + SIZE_OF_INT;
                input++;
                sequenceStore.storeSequence(inputBase, anchor, (int) (input - anchor), 0, matchLength - MIN_MATCH);
            }
            else {
                /* check prefix long match */
                long longMatchAdress = baseAddress + longMatchOffset;

                // TODO replace longMatchOffset > prefixLowestIndex with longMatchAddres > prefixLowest?
                if (longMatchOffset > prefixLowestIndex && UNSAFE.getLong(inputBase, longMatchAdress) == UNSAFE.getLong(inputBase, input)) {
                    matchLength = count(inputBase, input + SIZE_OF_LONG, longMatchAdress + SIZE_OF_LONG, inputEnd) + SIZE_OF_LONG;
                    offset = (int) (input - longMatchAdress);
                    while (input > anchor && longMatchAdress > prefixLowest && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, longMatchAdress - 1)) {
                        input--;
                        longMatchAdress--;
                        matchLength++;
                    }
                }
                else {
                    /* check prefix short match */
                    long shortMatchAddress = baseAddress + shortMatchOffset;
                    // TODO replace shortMatchOffset > prefixLowestIndex  with shortMatchAddres > prefixLowest?
                    if (shortMatchOffset > prefixLowestIndex && UNSAFE.getInt(inputBase, shortMatchAddress) == UNSAFE.getInt(inputBase, input)) {
                        int hash2 = (int) hash8(UNSAFE.getLong(inputBase, input + 1), longHashBits);
                        int matchOffset2 = longHashTable[hash2];
                        longHashTable[hash2] = current + 1;

                        /* check prefix long +1 match */
                        long matchAddress2 = baseAddress + matchOffset2;
                        if (matchOffset2 > prefixLowestIndex && UNSAFE.getLong(inputBase, matchAddress2) == UNSAFE.getLong(inputBase, input + 1)) {
                            matchLength = count(inputBase, input + 1 + SIZE_OF_LONG, matchAddress2 + SIZE_OF_LONG, inputEnd) + SIZE_OF_LONG;
                            input++;
                            offset = (int) (input - matchAddress2);
                            while (input > anchor && matchAddress2 > prefixLowest && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, matchAddress2 - 1)) {
                                input--;
                                matchAddress2--;
                                matchLength++;
                            }
                        }
                        else {
                            /* if no long +1 match, explore the short match we found */
                            matchLength = count(inputBase, input + SIZE_OF_INT, shortMatchAddress + SIZE_OF_INT, inputEnd) + SIZE_OF_INT;
                            offset = (int) (input - shortMatchAddress);
                            while (input > anchor && shortMatchAddress > prefixLowest && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, shortMatchAddress - 1)) {
                                input--;
                                shortMatchAddress--;
                                matchLength++;
                            }
                        }
                    }
                    else {
                        input += ((input - anchor) >> SEARCH_STRENGTH) + 1;
                        continue;
                    }
                }

                offset2 = offset1;
                offset1 = offset;

                sequenceStore.storeSequence(inputBase, anchor, (int) (input - anchor), offset + REP_MOVE, matchLength - MIN_MATCH);
            }

            input += matchLength;
            anchor = input;

            if (input <= inputLimit) {
                /* Fill Table */
                longHashTable[(int) hash8(UNSAFE.getLong(inputBase, baseAddress + current + 2), longHashBits)] = current + 2;
                shortHashTable[hash(inputBase, baseAddress + current + 2, shortHashBits, matchSearchLength)] = current + 2;

                longHashTable[(int) hash8(UNSAFE.getLong(inputBase, input - 2), longHashBits)] = (int) (input - 2 - baseAddress);
                shortHashTable[hash(inputBase, input - 2, shortHashBits, matchSearchLength)] = (int) (input - 2 - baseAddress);

                while (input <= inputLimit && offset2 > 0 && UNSAFE.getInt(inputBase, input) == UNSAFE.getInt(inputBase, input - offset2)) {
                    int repetitionLength = count(inputBase, input + SIZE_OF_INT, input + SIZE_OF_INT - offset2, inputEnd) + SIZE_OF_INT;

                    /* swap offset2 <=> offset1 */
                    int temp = offset2;
                    offset2 = offset1;
                    offset1 = temp;

                    shortHashTable[hash(inputBase, input, shortHashBits, matchSearchLength)] = (int) (input - baseAddress);
                    longHashTable[(int) hash8(UNSAFE.getLong(inputBase, input), longHashBits)] = (int) (input - baseAddress);

                    sequenceStore.storeSequence(inputBase, anchor, 0, 0, repetitionLength - MIN_MATCH);

                    input += repetitionLength;
                    anchor = input;
                }
            }
        }

        /* save reps for next block */
        rep[0] = offset1 != 0 ? offset1 : offsetSaved;
        rep[1] = offset2 != 0 ? offset2 : offsetSaved;

        /* Return the last literals size */
        return (int) (inputEnd - anchor);
    }

    // TODO: same as LZ4RawCompressor.count
    private static int count(Object inputBase, final long start, final long matchStart, final long matchLimit)
    {
        long current = start;
        long match = matchStart;

        // first, compare long at a time
        while (current < matchLimit - (SIZE_OF_LONG - 1)) {
            long diff = UNSAFE.getLong(inputBase, match) ^ UNSAFE.getLong(inputBase, current);
            if (diff != 0) {
                current += Long.numberOfTrailingZeros(diff) >> 3;
                return (int) (current - start);
            }

            current += SIZE_OF_LONG;
            match += SIZE_OF_LONG;
        }

        if (current < matchLimit - (SIZE_OF_INT - 1) && UNSAFE.getInt(inputBase, match) == UNSAFE.getInt(inputBase, current)) {
            current += SIZE_OF_INT;
            match += SIZE_OF_INT;
        }

        if (current < matchLimit - (SIZE_OF_SHORT - 1) && UNSAFE.getShort(inputBase, match) == UNSAFE.getShort(inputBase, current)) {
            current += SIZE_OF_SHORT;
            match += SIZE_OF_SHORT;
        }

        if (current < matchLimit && UNSAFE.getByte(inputBase, match) == UNSAFE.getByte(inputBase, current)) {
            ++current;
        }

        return (int) (current - start);
    }

    private static int hash(Object inputBase, long inputAddress, int bits, int matchSearchLength)
    {
        switch (matchSearchLength) {
            case 8:
                return (int) (hash8(UNSAFE.getLong(inputBase, inputAddress), bits));
            case 7:
                return (int) (hash7(UNSAFE.getLong(inputBase, inputAddress), bits));
            case 6:
                return (int) (hash6(UNSAFE.getLong(inputBase, inputAddress), bits));
            case 5:
                return (int) (hash5(UNSAFE.getLong(inputBase, inputAddress), bits));
            default:
                return hash4(UNSAFE.getInt(inputBase, inputAddress), bits);
        }
    }

    private static final int PRIME_4_BYTES = 0x9E3779B1;
    private static final long PRIME_5_BYTES = 0xCF1BBCDCBBL;
    private static final long PRIME_6_BYTES = 0xCF1BBCDCBF9BL;
    private static final long PRIME_7_BYTES = 0xCF1BBCDCBFA563L;
    private static final long PRIME_8_BYTES = 0xCF1BBCDCB7A56463L;

    private static int hash4(int value, int bits)
    {
        return (value * PRIME_4_BYTES) >>> (Integer.SIZE - bits);
    }

    private static long hash5(long value, int bits)
    {
//        DebugLog.print("hash5: value: %d, bits: %d", value, bits);
        return ((value << (Long.SIZE - 40)) * PRIME_5_BYTES) >>> (Long.SIZE - bits);
    }

    private static long hash6(long value, int bits)
    {
        return ((value << (Long.SIZE - 48)) * PRIME_6_BYTES) >>> (Long.SIZE - bits);
    }

    private static long hash7(long value, int bits)
    {
        return ((value << (Long.SIZE - 56)) * PRIME_7_BYTES) >>> (Long.SIZE - bits);
    }

    private static long hash8(long value, int bits)
    {
        return (value * PRIME_8_BYTES) >>> (Long.SIZE - bits);
    }

    public static void main(String[] args)
            throws IOException
    {
        byte[] data = Files.readAllBytes(Paths.get("testdata", "silesia", "xml"));
        CompressionParameters parameters = CompressionParameters.compute(0, data.length);

        CompressionContext context = new CompressionContext(parameters, data.length);

        DoubleFastBlockCompressor compressor = new DoubleFastBlockCompressor();
        System.err.println(compressor.compressBlock(data, ARRAY_BYTE_BASE_OFFSET, context.blockSize, context.sequenceStore, context.matchState, context.blockState.next.rep, parameters));

        System.err.println(context.sequenceStore.sequenceCount);
        System.err.println(context.sequenceStore.literalsOffset);
    }
}
