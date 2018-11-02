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
        int longShift = Long.SIZE - parameters.getHashLog();

        int[] shortHashTable = matchState.chainTable;
        int shortShift = Long.SIZE - parameters.getChainLog();

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

//        DebugLog.print("prefixLowestIndex: %d, hBitsL: %d, hBitsS: %d, srcSize: %d, offset_1: %d, offset_2: %d, mls: %d", prefixLowestIndex, longHashBits, shortHashBits, inputSize, offset1, offset2, matchSearchLength);

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

//        int loop1 = 0;
//        int loop2 = 0;
//        int loop3 = 0;
//        int loop4 = 0;
//        int loop5 = 0;
//        int loop6 = 0;

        while (input < inputLimit) {   /* < instead of <=, because repcode check at (input+1) */
//            loop1++;

            int shortHash = hash(inputBase, input, shortShift, matchSearchLength);
            long shortMatchAddress = baseAddress + shortHashTable[shortHash];

            int longHash = (hash8(UNSAFE.getLong(inputBase, input), longShift));
            long longMatchAddress = baseAddress + longHashTable[longHash];

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
                if (longMatchAddress > prefixLowest && UNSAFE.getLong(inputBase, longMatchAddress) == UNSAFE.getLong(inputBase, input)) {
                    matchLength = count(inputBase, input + SIZE_OF_LONG, longMatchAddress + SIZE_OF_LONG, inputEnd) + SIZE_OF_LONG;
                    offset = (int) (input - longMatchAddress);
                    while (input > anchor && longMatchAddress > prefixLowest && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, longMatchAddress - 1)) {
                        input--;
                        longMatchAddress--;
                        matchLength++;
//                        loop2++;
                    }
                }
                else {
                    /* check prefix short match */
                    if (shortMatchAddress > prefixLowest && UNSAFE.getInt(inputBase, shortMatchAddress) == UNSAFE.getInt(inputBase, input)) {
//                    if (shortMatchOffset > prefixLowestIndex && UNSAFE.getInt(inputBase, shortMatchAddress) == UNSAFE.getInt(inputBase, input)) {
                        int hash2 = hash8(UNSAFE.getLong(inputBase, input + 1), longShift);
                        long matchAddress2 = baseAddress + longHashTable[hash2];
                        longHashTable[hash2] = current + 1;

                        /* check prefix long +1 match */
                        if (matchAddress2 > prefixLowest && UNSAFE.getLong(inputBase, matchAddress2) == UNSAFE.getLong(inputBase, input + 1)) {
                            matchLength = count(inputBase, input + 1 + SIZE_OF_LONG, matchAddress2 + SIZE_OF_LONG, inputEnd) + SIZE_OF_LONG;
                            input++;
                            offset = (int) (input - matchAddress2);
                            while (input > anchor && matchAddress2 > prefixLowest && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, matchAddress2 - 1)) {
                                input--;
                                matchAddress2--;
                                matchLength++;
//                                loop4++;
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
//                                loop5++;
                            }
                        }
                    }
                    else {
                        input += ((input - anchor) >> SEARCH_STRENGTH) + 1;
//                        loop3++;
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
                int b = current + 2;
                long a = baseAddress + b;
                longHashTable[hash8(UNSAFE.getLong(inputBase, a), longShift)] = b;
                shortHashTable[hash(inputBase, a, shortShift, matchSearchLength)] = b;

                long z = input - 2;
                int x = (int) (z - baseAddress);
                longHashTable[hash8(UNSAFE.getLong(inputBase, z), longShift)] = x;
                shortHashTable[hash(inputBase, z, shortShift, matchSearchLength)] = x;

                while (input <= inputLimit && offset2 > 0 && UNSAFE.getInt(inputBase, input) == UNSAFE.getInt(inputBase, input - offset2)) {
                    int repetitionLength = count(inputBase, input + SIZE_OF_INT, input + SIZE_OF_INT - offset2, inputEnd) + SIZE_OF_INT;

                    /* swap offset2 <=> offset1 */
                    int temp = offset2;
                    offset2 = offset1;
                    offset1 = temp;

                    int delta = (int) (input - baseAddress);
                    shortHashTable[hash(inputBase, input, shortShift, matchSearchLength)] = delta;
                    longHashTable[hash8(UNSAFE.getLong(inputBase, input), longShift)] = delta;

                    sequenceStore.storeSequence(inputBase, anchor, 0, 0, repetitionLength - MIN_MATCH);

                    input += repetitionLength;
                    anchor = input;
//                    loop6++;
                }
            }
        }

        /* save reps for next block */
        rep[0] = offset1 != 0 ? offset1 : offsetSaved;
        rep[1] = offset2 != 0 ? offset2 : offsetSaved;

//        DebugLog.print("loop1: %d", loop1);
//        DebugLog.print("loop2: %d", loop2);
//        DebugLog.print("loop3: %d", loop3);
//        DebugLog.print("loop4: %d", loop4);
//        DebugLog.print("loop5: %d", loop5);
//        DebugLog.print("loop6: %d", loop6);
        
        /* Return the last literals size */
        return (int) (inputEnd - anchor);
    }

    // TODO: same as LZ4RawCompressor.count
    public static int count(Object inputBase, final long start, final long matchStart, final long matchLimit)
    {
        long current = start;
        long match = matchStart;

        int inputSize = (int) (matchLimit - start);

        // first, compare long at a time
        int i = 0;
        while (i < inputSize - (SIZE_OF_LONG - 1)) {
            long diff = UNSAFE.getLong(inputBase, match) ^ UNSAFE.getLong(inputBase, current);
            if (diff != 0) {
                return i + (Long.numberOfTrailingZeros(diff) >> 3);
            }

            i += SIZE_OF_LONG;
            current += SIZE_OF_LONG;
            match += SIZE_OF_LONG;
        }

        return countTail(inputBase, start, matchLimit, current, match);
    }

    private static int countTail(Object inputBase, long start, long matchLimit, long current, long match)
    {
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

    public static int count2(Object inputBase, final long start, final long matchStart, final long matchLimit)
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

    private static int hash(Object inputBase, long inputAddress, int shift, int matchSearchLength)
    {
//        switch (matchSearchLength) {
//            case 8:
//                return (int) (hash8(UNSAFE.getLong(inputBase, inputAddress), shift));
//            case 7:
//                return (int) (hash7(UNSAFE.getLong(inputBase, inputAddress), shift));
//            case 6:
//                return (int) (hash6(UNSAFE.getLong(inputBase, inputAddress), shift));
//            case 5:
                return (int) (hash5(UNSAFE.getLong(inputBase, inputAddress), shift));
//            default:
//                return hash4(UNSAFE.getInt(inputBase, inputAddress), shift);
//        }
    }

    private static final int PRIME_4_BYTES = 0x9E3779B1;
    private static final long PRIME_5_BYTES = 0xCF1BBCDCBBL;
    private static final long PRIME_6_BYTES = 0xCF1BBCDCBF9BL;
    private static final long PRIME_7_BYTES = 0xCF1BBCDCBFA563L;
    private static final long PRIME_8_BYTES = 0xCF1BBCDCB7A56463L;

    private static int hash4(int value, int shift)
    {
        return (value * PRIME_4_BYTES) >>> (Integer.SIZE - (64 - shift));
    }

    private static long hash5(long value, int shift)
    {
//        DebugLog.print("hash5: value: %d, bits: %d", value, bits);
        return ((value << (Long.SIZE - 40)) * PRIME_5_BYTES) >>> shift;
    }

    private static long hash6(long value, int shift)
    {
        return ((value << (Long.SIZE - 48)) * PRIME_6_BYTES) >>> shift;
    }

    private static long hash7(long value, int shift)
    {
        return ((value << (Long.SIZE - 56)) * PRIME_7_BYTES) >>> shift;
    }

//    private static long hash8(long value, int bits)
//    {
//        return (value * PRIME_8_BYTES) >>> (Long.SIZE - bits);
//    }

    private static int hash8(long value, int shift)
    {
        return (int) ((value * PRIME_8_BYTES) >>> shift);
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
