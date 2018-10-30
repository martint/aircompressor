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

class CompressedBlockState
{
    public int[] rep = new int[] {1, 4, 8};
    public Entropy entropy = new Entropy(); // TODO

    public enum RepeatMode {
        REPEAT_NONE,   /* Cannot use the previous table */
        REPEAT_CHECK,  /* Can use the previous table but it must be checked */
        REPEAT_VALID  /* Can use the previous table and it is asumed to be valid */
    }

    public static class Entropy
    {
        HuffmanTable huffman = new HuffmanTable(new HuffmanCompressionTable());

        FseTable literalLengths = new FseTable(new FseCompressionTable(Constants.LITERALS_LENGTH_FSE_LOG, Constants.MAX_LITERALS_LENGTH_SYMBOL));
        FseTable offsetCodes = new FseTable(new FseCompressionTable(Constants.OFFSET_CODES_FSE_LOG, Constants.MAX_OFFSET_CODE_SYMBOL));
        FseTable matchLengths = new FseTable(new FseCompressionTable(Constants.MATCH_LENGTH_FSE_LOG, Constants.MAX_MATCH_LENGTH_SYMBOL));

        public void copy(Entropy from)
        {
            literalLengths.copy(from.literalLengths);
            offsetCodes.copy(from.offsetCodes);
            matchLengths.copy(from.matchLengths);
        }
    }

    public static class FseTable
    {
        public RepeatMode repeatMode;
        public FseCompressionTable table;

        public FseTable(FseCompressionTable table)
        {
            this.repeatMode = RepeatMode.REPEAT_NONE;
            this.table = table;
        }

        public void reset()
        {
            repeatMode = RepeatMode.REPEAT_NONE;
        }

        public void copy(FseTable other)
        {
            table.copy(other.table);
        }
    }

    public static class HuffmanTable
    {
        public RepeatMode repeatMode;
        public HuffmanCompressionTable table;

        public HuffmanTable(HuffmanCompressionTable table)
        {
            this.repeatMode = RepeatMode.REPEAT_NONE;
            this.table = table;
        }

        public void copy(HuffmanTable other)
        {
            table.copyFrom(other.table);
        }

    }
}
