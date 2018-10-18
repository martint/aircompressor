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
package io.airlift.compress.fse;

import static io.airlift.compress.fse.UnsafeUtil.UNSAFE;
import static io.airlift.compress.fse.Util.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

class TableSerializer
{
    public static final int MAX_TABLE_LOG = 12;
    public static final int MIN_TABLE_LOG = 5;

    public static final int FSE_MAX_SYMBOL_VALUE = 255;

    public int readNormalizedCounts(short[] normalizedCounters, Object inputBase, long inputAddress, long inputLimit, int maxSymbol, int maxTableLog)
    {
        // read table headers
        long input = inputAddress;
        verify(inputLimit - inputAddress >= 4, input, "Not enough input bytes");

        int threshold;
        int symbolNumber = 0;
        boolean previousIsZero = false;

        int bitStream = UNSAFE.getInt(inputBase, input);

        int tableLog = (bitStream & 0xF) + MIN_TABLE_LOG;

        int numberOfBits = tableLog + 1;
        bitStream >>>= 4;
        int bitCount = 4;

        verify(tableLog <= maxTableLog, input, "FSE table size exceeds maximum allowed size");

        int remaining = (1 << tableLog) + 1;
        threshold = 1 << tableLog;

        while (remaining > 1 && symbolNumber <= maxSymbol) {
            if (previousIsZero) {
                int n0 = symbolNumber;
                while ((bitStream & 0xFFFF) == 0xFFFF) {
                    n0 += 24;
                    if (input < inputLimit - 5) {
                        input += 2;
                        bitStream = (UNSAFE.getInt(inputBase, input) >>> bitCount);
                    }
                    else {
                        // end of bit stream
                        bitStream >>>= 16;
                        bitCount += 16;
                    }
                }
                while ((bitStream & 3) == 3) {
                    n0 += 3;
                    bitStream >>>= 2;
                    bitCount += 2;
                }
                n0 += bitStream & 3;
                bitCount += 2;

                verify(n0 <= maxSymbol, input, "Symbol larger than max value");

                while (symbolNumber < n0) {
                    normalizedCounters[symbolNumber++] = 0;
                }
                if ((input <= inputLimit - 7) || (input + (bitCount >>> 3) <= inputLimit - 4)) {
                    input += bitCount >>> 3;
                    bitCount &= 7;
                    bitStream = UNSAFE.getInt(inputBase, input) >>> bitCount;
                }
                else {
                    bitStream >>>= 2;
                }
            }

            short max = (short) ((2 * threshold - 1) - remaining);
            short count;

            if ((bitStream & (threshold - 1)) < max) {
                count = (short) (bitStream & (threshold - 1));
                bitCount += numberOfBits - 1;
            }
            else {
                count = (short) (bitStream & (2 * threshold - 1));
                if (count >= threshold) {
                    count -= max;
                }
                bitCount += numberOfBits;
            }
            count--;  // extra accuracy

            remaining -= Math.abs(count);
            normalizedCounters[symbolNumber++] = count;
            previousIsZero = count == 0;
            while (remaining < threshold) {
                numberOfBits--;
                threshold >>>= 1;
            }

            if ((input <= inputLimit - 7) || (input + (bitCount >> 3) <= inputLimit - 4)) {
                input += bitCount >>> 3;
                bitCount &= 7;
            }
            else {
                bitCount -= (int) (8 * (inputLimit - 4 - input));
                input = inputLimit - 4;
            }
            bitStream = UNSAFE.getInt(inputBase, input) >>> (bitCount & 31);
        }

        verify(remaining == 1 && bitCount <= 32, input, "Input is corrupted");

        maxSymbol = symbolNumber - 1;
        verify(maxSymbol <= FSE_MAX_SYMBOL_VALUE, input, "Max symbol value too large (too many symbols for FSE)");

        input += (bitCount + 7) >> 3;
        
        return (int) (input - inputAddress);
    }

    public static int writeNormalizedCounts(Object outputBase, long outputAddress, int outputSize, short[] normalizedCounts, int maxSymbol, int tableLog)
    {
        verify(tableLog <= MAX_TABLE_LOG, "FSE table too large");
        verify(tableLog >= MIN_TABLE_LOG, "FSE table too small");

        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        int tableSize = 1 << tableLog;

        int bitCount = 0;

        // encode table size
        int bitStream = (tableLog - MIN_TABLE_LOG);
        bitCount += 4;

        int remaining = tableSize + 1; // +1 for extra accuracy
        int threshold = tableSize;
        int nbBits = tableLog + 1;

        int symbol = 0;

        boolean previous0 = false;
        while (remaining > 1) {
            if (previous0) {
                // From RFC 8478:
                //   When a symbol has a probability of zero, it is followed by a 2-bit
                //   repeat flag.  This repeat flag tells how many probabilities of zeroes
                //   follow the current one.  It provides a number ranging from 0 to 3.
                //   If it is a 3, another 2-bit repeat flag follows, and so on.
                int start = symbol;

                // find run of symbols with count 0
                while (normalizedCounts[symbol] == 0) {
                    symbol++;
                }

                // encode in batches if 8 repeat sequences in one shot (representing 24 symbols total)
                while (symbol >= start + 24) {
                    start += 24;
                    bitStream |= (0b11_11_11_11_11_11_11_11 << bitCount);
                    verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                    // TODO: putShort?
                    UNSAFE.putByte(outputBase, output, (byte) bitStream);
                    UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                    output += SIZE_OF_SHORT;

                    // flush now, so no need to increase bitCount by 16
                    bitStream >>>= Short.SIZE;
                }

                // encode remaining in batches of 3 symbols
                while (symbol >= start + 3) {
                    start += 3;
                    bitStream += 0b11 << bitCount;
                    bitCount += 2;
                }

                // encode tail
                bitStream += (symbol - start) << bitCount;
                bitCount += 2;

                // flush bitstream if necessary
                if (bitCount > 16) {
                    verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                    // TODO: putShort?
                    UNSAFE.putByte(outputBase, output, (byte) bitStream);
                    UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                    output += SIZE_OF_SHORT;

                    bitStream >>= Short.SIZE;
                    bitCount -= Short.SIZE;
                }
            }

            int count = normalizedCounts[symbol++];
            int max = (2 * threshold - 1) - remaining;
            remaining -= count < 0 ? -count : count;
            count++;   /* +1 for extra accuracy */
            if (count >= threshold) {
                count += max;
            }
            bitStream += count << bitCount;
            bitCount += nbBits;
            bitCount -= (count < max ? 1 : 0);
            previous0 = (count == 1);

            verify(remaining >= 1, "Error"); // TODO

            while (remaining < threshold) {
                nbBits--;
                threshold >>= 1;
            }

            // flush bitstream if necessary
            if (bitCount > 16) {
                verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                // TODO: putShort?
                UNSAFE.putByte(outputBase, output, (byte) bitStream);
                UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                output += SIZE_OF_SHORT;

                bitStream >>= Short.SIZE;
                bitCount -= Short.SIZE;
            }
        }

        // flush remaining bitstream
        verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");
        // TODO: putShort?
        UNSAFE.putByte(outputBase, output, (byte) bitStream);
        UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
        output += (bitCount + 7) / 8;

        verify(symbol <= maxSymbol + 1, "Error"); // TODO

        return (int) (output - outputAddress);
    }

//    public static void main(String[] args)
//    {
//        int maxSymbol = 255;
//
//        int[] counts = generateCounts(maxSymbol);
//        int total = Arrays.stream(counts).sum();
//
//        short[] original = new short[counts.length];
//
//        int tableLog = MAX_TABLE_LOG;
//        SequenceCompressor.normalizeCounts(original, tableLog, counts, total, maxSymbol);
//
//        byte[] buffer = new byte[1000];
//        int written = writeNormalizedCounts(buffer, ARRAY_BYTE_BASE_OFFSET, buffer.length, original, maxSymbol, tableLog);
//
//
//        short[] decoded = new short[FSE_MAX_SYMBOL_VALUE + 1];
//        FseTableReader2 reader = new FseTableReader2();
//        reader.readNormalizedCounters(decoded, buffer, ARRAY_BYTE_BASE_OFFSET, ARRAY_BYTE_BASE_OFFSET + written, maxSymbol, FiniteStateEntropy.MAX_TABLE_LOG);
//
//        for (int i = 0; i < original.length; i++) {
//            if (original[i] != decoded[i]) {
//                throw new IllegalStateException("Mismatch at " + i);
//            }
//        }
//    }
//
//    private static int[] generateCounts(int maxSymbol)
//    {
//        int[] counts = new int[maxSymbol + 1];
//        for (int i = 0; i < counts.length; i++) {
////            counts[i] = Math.max(0, (int) (random.nextGaussian() * 1000));
//            counts[i] = i;
//        }
//        return counts;
//    }
}
