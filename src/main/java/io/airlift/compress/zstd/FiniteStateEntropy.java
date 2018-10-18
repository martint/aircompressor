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

import static io.airlift.compress.zstd.BitStream.peekBits;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;
import static io.airlift.compress.zstd.Constants.SIZE_OF_INT;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class FiniteStateEntropy
{
    public static final int MAX_TABLE_LOG = 12;
    public static final int MIN_TABLE_LOG = 5;
    public static final int DEFAULT_TABLE_LOG = 11;

    public static final int FSE_NCOUNTBOUND = 512; // TODO: rename

    public static int decompress(FiniteStateEntropy.Table table, final Object inputBase, final long inputAddress, final long inputLimit, byte[] outputBuffer)
    {
        final Object outputBase = outputBuffer;
        final long outputAddress = ARRAY_BYTE_BASE_OFFSET;
        final long outputLimit = outputAddress + outputBuffer.length;

        long input = inputAddress;
        long output = outputAddress;

        // initialize bit stream
        BitStream.Initializer initializer = new BitStream.Initializer(inputBase, input, inputLimit);
        initializer.initialize();
        int bitsConsumed = initializer.getBitsConsumed();
        long currentAddress = initializer.getCurrentAddress();
        long bits = initializer.getBits();

        // initialize first FSE stream
        int state1 = (int) peekBits(bitsConsumed, bits, table.log2Size);
        bitsConsumed += table.log2Size;

        BitStream.Loader loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
        loader.load();
        bits = loader.getBits();
        bitsConsumed = loader.getBitsConsumed();
        currentAddress = loader.getCurrentAddress();

        // initialize second FSE stream
        int state2 = (int) peekBits(bitsConsumed, bits, table.log2Size);
        bitsConsumed += table.log2Size;

        loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
        loader.load();
        bits = loader.getBits();
        bitsConsumed = loader.getBitsConsumed();
        currentAddress = loader.getCurrentAddress();

        byte[] symbols = table.symbol;
        byte[] numbersOfBits = table.numberOfBits;
        int[] newStates = table.newState;

        // decode 4 symbols per loop
        while (output <= outputLimit - 4) {
            int numberOfBits;

            UNSAFE.putByte(outputBase, output, symbols[state1]);
            numberOfBits = numbersOfBits[state1];
            state1 = (int) (newStates[state1] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            UNSAFE.putByte(outputBase, output + 1, symbols[state2]);
            numberOfBits = numbersOfBits[state2];
            state2 = (int) (newStates[state2] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            UNSAFE.putByte(outputBase, output + 2, symbols[state1]);
            numberOfBits = numbersOfBits[state1];
            state1 = (int) (newStates[state1] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            UNSAFE.putByte(outputBase, output + 3, symbols[state2]);
            numberOfBits = numbersOfBits[state2];
            state2 = (int) (newStates[state2] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            output += SIZE_OF_INT;

            loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
            boolean done = loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentAddress = loader.getCurrentAddress();
            if (done) {
                break;
            }
        }

        while (true) {
            verify(output <= outputLimit - 2, input, "Output buffer is too small");
            UNSAFE.putByte(outputBase, output++, symbols[state1]);
            int numberOfBits = numbersOfBits[state1];
            state1 = (int) (newStates[state1] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
            loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentAddress = loader.getCurrentAddress();

            if (loader.isOverflow()) {
                UNSAFE.putByte(outputBase, output++, symbols[state2]);
                break;
            }

            verify(output <= outputLimit - 2, input, "Output buffer is too small");
            UNSAFE.putByte(outputBase, output++, symbols[state2]);
            int numberOfBits1 = numbersOfBits[state2];
            state2 = (int) (newStates[state2] + peekBits(bitsConsumed, bits, numberOfBits1));
            bitsConsumed += numberOfBits1;

            loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
            loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentAddress = loader.getCurrentAddress();

            if (loader.isOverflow()) {
                UNSAFE.putByte(outputBase, output++, symbols[state1]);
                break;
            }
        }

        return (int) (output - outputAddress);
    }

    public static int calculateStep(int tableSize)
    {
        return (tableSize >>> 1) + (tableSize >>> 3) + 3;
    }

    public static int spreadSymbols(short[] normalizedCounters, int symbolCount, int tableSize, int highThreshold, byte[] symbols)
    {
        int mask = tableSize - 1;
        int step = calculateStep(tableSize);
        int position = 0;
        for (byte symbol = 0; symbol < symbolCount; symbol++) {
            for (int i = 0; i < normalizedCounters[symbol]; i++) {
                symbols[position] = symbol;
                do {
                    position = (position + step) & mask;
                }
                while (position > highThreshold);
            }
        }
        return position;
    }

    public static int optimalTableLog(int maxTableLog, int srcSize, int maxSymbolValue)
    {
        if (srcSize <= 1) {
            throw new IllegalArgumentException(); // not supported. Use RLE instead
        }

        int tableLog = maxTableLog;
        if (tableLog == 0) {
            tableLog = DEFAULT_TABLE_LOG;
        }

        int maxBitsSrc = Util.highestBit((srcSize - 1)) - 2;
        if (maxBitsSrc < tableLog) {
            tableLog = maxBitsSrc;   /* Accuracy can be reduced */
        }

        int minBits = minTableLog(srcSize, maxSymbolValue);
        if (minBits > tableLog) {
            tableLog = minBits;   /* Need a minimum to safely represent all symbol values */
        }

        if (tableLog < MIN_TABLE_LOG) {
            tableLog = MIN_TABLE_LOG;
        }

        if (tableLog > MAX_TABLE_LOG) {
            tableLog = MAX_TABLE_LOG;
        }

        return tableLog;
    }

    /* provides the minimum logSize to safely represent a distribution */
    public static int minTableLog(int inputSize, int maxSymbolValue)
    {
        if (inputSize <= 1) {
            throw new IllegalArgumentException("Not supported. RLE should be used instead"); // TODO
        }

        int minBitsSrc = Util.highestBit((inputSize - 1)) + 1;
        int minBitsSymbols = Util.highestBit(maxSymbolValue) + 2;
        return Math.min(minBitsSrc, minBitsSymbols);
    }

    public static final class Table
    {
        int log2Size;
        final int[] newState;
        final byte[] symbol;
        final byte[] numberOfBits;

        public Table(int log2Capacity)
        {
            int capacity = 1 << log2Capacity;
            newState = new int[capacity];
            symbol = new byte[capacity];
            numberOfBits = new byte[capacity];
        }

        public Table(int log2Size, int[] newState, byte[] symbol, byte[] numberOfBits)
        {
            int size = 1 << log2Size;
            if (newState.length != size || symbol.length != size || numberOfBits.length != size) {
                throw new IllegalArgumentException("Expected arrays to match provided size");
            }

            this.log2Size = log2Size;
            this.newState = newState;
            this.symbol = symbol;
            this.numberOfBits = numberOfBits;
        }
    }
}
