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

public class HuffmanCompressionContext
{
    public static final int HUF_SYMBOLVALUE_MAX = 255;
    public static final int MAX_SYMBOL_COUNT = HUF_SYMBOLVALUE_MAX + 1;
    public static final int HUF_CTABLE_WORKSPACE_SIZE_U32 = (2 * MAX_SYMBOL_COUNT - 1) + 1; // number of nodes in binary tree with MAX_SYMBOL_COUNT leaves + 1 for sentinel

    NodeTable nodeTable = new NodeTable(HUF_CTABLE_WORKSPACE_SIZE_U32);
    HuffmanCompressionTable table = new HuffmanCompressionTable();

    CompressedBlockState.RepeatMode repeat = CompressedBlockState.RepeatMode.REPEAT_NONE;

    public static class NodeTable
    {
        int[] count;
        short[] parents; // U16 TODO: int?
        byte[] bytes;
        byte[] numberOfBits;

        public NodeTable(int size)
        {
            count = new int[size];
            parents = new short[size];
            bytes = new byte[size];
            numberOfBits = new byte[size];
        }

        public void reset()
        {
            Arrays.fill(count, 0);
            Arrays.fill(parents, (short) 0);
            Arrays.fill(bytes, (byte) 0);
            Arrays.fill(numberOfBits, (byte) 0);
        }

        public void copyNode(int from, int to)
        {
            count[to] = count[from];
            parents[to] = parents[from];
            bytes[to] = bytes[from];
            numberOfBits[to] = numberOfBits[from];
        }
    }
}
