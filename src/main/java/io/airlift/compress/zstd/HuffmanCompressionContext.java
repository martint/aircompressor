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

public class HuffmanCompressionContext
{
    NodeTable nodeTable = new NodeTable((2 * Huffman.MAX_SYMBOL_COUNT - 1) + 1); // number of nodes in binary tree with MAX_SYMBOL_COUNT leaves + 1 for sentinel
    HuffmanCompressionTable table = new HuffmanCompressionTable();
    CompressedBlockState.RepeatMode repeat = CompressedBlockState.RepeatMode.REPEAT_NONE;
}
