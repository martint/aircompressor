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

public class FseCompressionTable
{
    final int log2Size;
    final int maxSymbol;

    public final short[] table;
    public final int[] deltaNumberOfBits;
    public final int[] deltaFindState;

    public FseCompressionTable(int maxTableLog, int maxSymbolValue)
    {
        log2Size = maxTableLog;
        maxSymbol = maxSymbolValue;
        table = new short[1 << maxTableLog];
        deltaNumberOfBits = new int[maxSymbolValue + 1];
        deltaFindState = new int[maxSymbolValue + 1];
    }

    public static FseCompressionTable makeRleTable(int symbol)
    {
        FseCompressionTable table = new FseCompressionTable(0, symbol);

        table.table[0] = 0;
        table.table[1] = 0;

        table.deltaFindState[symbol] = 0;
        table.deltaNumberOfBits[symbol] = 0;

        return table;
    }
}
