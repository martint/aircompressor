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

import java.util.Arrays;

public final class HuffmanCompressionTable
{
    final short[] values;
    final byte[] numberOfBits;

    public HuffmanCompressionTable(int capacity)
    {
        this.values = new short[capacity + 1];
        this.numberOfBits = new byte[capacity + 1];
    }

    /**
     * Can this table encode all symbols with non-zero count?
     */
    public boolean isValid(int[] counts, int maxSymbol)
    {
        for (int symbol = 0; symbol <= maxSymbol; ++symbol) {
            if (counts[symbol] != 0 && numberOfBits[symbol] == 0) {
                return false;
            }
        }
        return true;
    }

    public int estimateCompressedSize(int[] counts, int maxSymbolValue)
    {
        int numberOfBits = 0;
        for (int symbol = 0; symbol <= maxSymbolValue; symbol++) {
            numberOfBits += this.numberOfBits[symbol] * counts[symbol];
            DebugLog.print("symbol %d: bits=%d, count=%d, total-so-far=%d", symbol, this.numberOfBits[symbol], counts[symbol], numberOfBits);
        }

        return numberOfBits >>> 3; // convert to bytes
    }

    public void trim(int maxSymbolValue)
    {
        Arrays.fill(values, maxSymbolValue + 1, values.length, (short) 0);
        Arrays.fill(numberOfBits, maxSymbolValue + 1, values.length, (byte) 0);
    }

    public void copyFrom(HuffmanCompressionTable other)
    {
        System.arraycopy(other.values, 0, values, 0, values.length);
        System.arraycopy(other.numberOfBits, 0, numberOfBits, 0, numberOfBits.length);
    }
}
