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

public class CompressionTable
{
    final int log2Size;
    final int maxSymbol;

    public final short[] nextState;
    public final int[] deltaNumberOfBits;
    public final int[] deltaFindState;

    public CompressionTable(int maxTableLog, int maxSymbolValue)
    {
        log2Size = maxTableLog;
        maxSymbol = maxSymbolValue;
        nextState = new short[1 << maxTableLog];
        deltaNumberOfBits = new int[maxSymbolValue + 1];
        deltaFindState = new int[maxSymbolValue + 1];
    }

    public static CompressionTable makeRleTable(int symbol)
    {
        CompressionTable table = new CompressionTable(0, symbol);

        table.nextState[0] = 0;
        table.nextState[1] = 0;

        table.deltaFindState[symbol] = 0;
        table.deltaNumberOfBits[symbol] = 0;

        return table;
    }
}
