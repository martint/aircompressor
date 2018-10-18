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
    int log2Size;
    int maxSymbol;

    public final short[] nextState;
    public final int[] deltaNumberOfBits;
    public final int[] deltaFindState;

    public FseCompressionTable(int maxTableLog, int maxSymbolValue)
    {
        log2Size = maxTableLog;
        maxSymbol = maxSymbolValue;
        nextState = new short[1 << maxTableLog];
        deltaNumberOfBits = new int[maxSymbolValue + 1];
        deltaFindState = new int[maxSymbolValue + 1];
    }

    public static FseCompressionTable makeRleTable(FseCompressionTable table, int symbol)
    {
        table.log2Size = 0;

        table.nextState[0] = 0;
        table.nextState[1] = 0;

        table.deltaFindState[symbol] = 0;
        table.deltaNumberOfBits[symbol] = 0;

        return table;
    }

    public void copy(FseCompressionTable other)
    {
        if (other.log2Size != log2Size || other.maxSymbol != maxSymbol) {
            throw new IllegalArgumentException();
        }

        System.arraycopy(other.deltaNumberOfBits, 0, deltaNumberOfBits, 0, other.deltaNumberOfBits.length);
        System.arraycopy(other.deltaFindState, 0, deltaFindState, 0, other.deltaFindState.length);
        System.arraycopy(other.nextState, 0, nextState, 0, other.nextState.length);
    }
}
