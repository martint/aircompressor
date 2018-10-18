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

import static io.airlift.compress.fse.Util.highestBit;

public final class DecompressionTable
{
    final int log2Size;
    final int[] nextStates;
    final byte[] symbols;
    final byte[] numberOfBits;

    public DecompressionTable(int maxSymbol, int tableLog, short[] normalizedCounters)
    {
        int tableSize = 1 << tableLog;

        log2Size = tableLog;
        symbols = new byte[tableSize];
        nextStates = new int[tableSize];
        numberOfBits = new byte[tableSize];

        int symbolCount = maxSymbol + 1;
        int highThreshold = tableSize - 1;

        short[] nextSymbol = new short[maxSymbol + 1];
        for (byte symbol = 0; symbol < symbolCount; symbol++) {
            if (normalizedCounters[symbol] == -1) {
                this.symbols[highThreshold--] = symbol;
                nextSymbol[symbol] = 1;
            }
            else {
                nextSymbol[symbol] = normalizedCounters[symbol];
            }
        }

        spreadSymbols(symbols, normalizedCounters, symbolCount, tableSize, highThreshold);

        for (int state = 0; state < tableSize; state++) {
            byte symbol = symbols[state];
            short nextState = nextSymbol[symbol]++;
            numberOfBits[state] = (byte) (tableLog - highestBit(nextState));
            nextStates[state] = (short) ((nextState << numberOfBits[state]) - tableSize);
        }
    }

    private static byte[] spreadSymbols(byte[] symbols, short[] normalizedCounters, int symbolCount, int tableSize, int highThreshold)
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

        // position != 0 => all cells have been visited due to table size (power of 2) and the constant in calculateStep not having a common factor.
        if (position != 0) {
            throw new IllegalArgumentException("normalizedCounters are invalid");
        }

        return symbols;
    }

    private static int calculateStep(int tableSize)
    {
        return (tableSize >>> 1) + (tableSize >>> 3) + 3;
    }
}
