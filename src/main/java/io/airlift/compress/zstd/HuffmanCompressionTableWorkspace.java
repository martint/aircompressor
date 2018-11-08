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

public class HuffmanCompressionTableWorkspace
{
    NodeTable nodeTable = new NodeTable((2 * Huffman.MAX_SYMBOL_COUNT - 1) + 1); // number of nodes in binary tree with MAX_SYMBOL_COUNT leaves + 1 for sentinel

    short[] numberOfBitsPerRank = new short[Huffman.MAX_TABLE_LOG + 1];
    short[] valuesPerRank = new short[Huffman.MAX_TABLE_LOG + 1];

    public void reset()
    {
        Arrays.fill(numberOfBitsPerRank, (short) 0);
        Arrays.fill(valuesPerRank, (short) 0);
    }
}
