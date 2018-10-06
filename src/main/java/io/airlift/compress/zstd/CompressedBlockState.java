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

    public static class Entropy
    {
        HuffmanTable huffman;
        FseTables fse;
    }

    public static class FseTables
    {
        public int litlength_repeatMode;
        public FseCompressionTable litlengthCTable;
        public int offcode_repeatMode;
        public FseCompressionTable offcodeCTable;
        public int matchlength_repeatMode;
        public FseCompressionTable matchlengthCTable;
    }

    public static class HuffmanTable
    {
//        typedef struct {
//            U32 CTable[HUF_CTABLE_SIZE_U32(255)];
//            HUF_repeat repeatMode;
//        } ZSTD_hufCTables_t;
    }
}
