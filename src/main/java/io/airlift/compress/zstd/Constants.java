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

public class Constants
{
    public static final int SIZE_OF_BYTE = 1;
    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;

    public static final int MAGIC_NUMBER = 0xFD2FB528;

    public static final int MIN_WINDOW_LOG = 10;
    public static final int MAX_WINDOW_LOG = 31;

    public static final int SIZE_OF_BLOCK_HEADER = 3;

    public static final int MIN_SEQUENCES_SIZE = 1;
    public static final int MIN_BLOCK_SIZE = 1 // block type tag
            + 1 // min size of raw or rle length header
            + MIN_SEQUENCES_SIZE;

    public static final int REP_CODE_COUNT = 3;

    // block types
    public static final int RAW_BLOCK = 0;
    public static final int RLE_BLOCK = 1;
    public static final int COMPRESSED_BLOCK = 2;

    static final int MAX_LITERALS_LENGTH_SYMBOL = 35;
    static final int MAX_MATCH_LENGTH_SYMBOL = 52;
    static final int MAX_OFFSET_CODE_SYMBOL = 28;

    public static final int MAX_BLOCK_SIZE = 1 << 17;

    private Constants()
    {
    }
}
