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

import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

//import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class SequenceStore
{
    // TODO: single contiguous buffer for all?

    public byte[] literalsBuffer;
    public int literalsOffset;

    public int[] offsets;
    public int[] literalLengths; // U16
    public int[] matchLengths;   // U16
    public int sequenceCount;

    public byte[] literalLengthCodes;
    public byte[] matchLengthCodes;
    public byte[] offsetCodes;

    public LongField longLengthField;
    public int longLengthPosition;

    public enum LongField {
        LITERAL, MATCH
    }

    public SequenceStore(int blockSize, int maxSequences)
    {
        offsets = new int[maxSequences];
        literalLengths = new int[maxSequences];
        matchLengths = new int[maxSequences];

        literalLengthCodes = new byte[maxSequences];
        matchLengthCodes = new byte[maxSequences];
        offsetCodes = new byte[maxSequences];

        literalsBuffer = new byte[blockSize];

        reset();
    }

    public void appendLiterals(Object inputBase, long inputAddress, int inputSize)
    {
        UNSAFE.copyMemory(inputBase, inputAddress, literalsBuffer, ARRAY_BYTE_BASE_OFFSET + literalsOffset, inputSize);
        literalsOffset += inputSize;
    }

    long start;

    public void storeSequence(Object literalBase, long literalAddress, int literalLength, int offsetCode, int matchLengthBase)
    {
        // TODO: store codes directly?

//        if (start == 0) {
//            start = literalAddress;
//        }
//        long index = literalAddress - start;
//        DebugLog.print("Cpos%7d :%3d literals, match%4d bytes at offCode%7d", index, literalLength, matchLengthBase + MIN_MATCH, offsetCode);

        // method 1
//        long input = literalAddress;
//        long output = literalsOffset + ARRAY_BYTE_BASE_OFFSET;
//        for (int i = 0; i < literalLength; i += SIZE_OF_LONG) {
//            UNSAFE.putLong(literalsBuffer, output, UNSAFE.getLong(literalBase, input));
//            input += SIZE_OF_LONG;
//            output += SIZE_OF_LONG;
//        }

        // method 2
//        final long outputLimit = ARRAY_BYTE_BASE_OFFSET + literalsOffset + literalLength;
//        long input = literalAddress;
//        long output = ARRAY_BYTE_BASE_OFFSET + literalsOffset;
//        do {
//            UNSAFE.putLong(literalsBuffer, output, UNSAFE.getLong(literalBase, input));
//            input += SIZE_OF_LONG;
//            output += SIZE_OF_LONG;
//        }
//        while (output < outputLimit);

        // method 3
//        UNSAFE.copyMemory(literalBase, literalAddress, literalsBuffer, ARRAY_BYTE_BASE_OFFSET + literalsOffset, literalLength);

        // method 4
        long input = literalAddress;
        long output = ARRAY_BYTE_BASE_OFFSET + literalsOffset;
        int copied = 0;
        do {
            UNSAFE.putLong(literalsBuffer, output, UNSAFE.getLong(literalBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
            copied += SIZE_OF_LONG;
        }
        while (copied < literalLength);

        literalsOffset += literalLength;

//        if (literalLength > 65535) {
//            longLengthField = LongField.LITERAL;
//            longLengthPosition = sequenceCount;
//        }
        literalLengths[sequenceCount] = literalLength;

        offsets[sequenceCount] = offsetCode + 1;

//        if (matchLengthBase > 65535) {
//            longLengthField = LongField.MATCH;
//            longLengthPosition = sequenceCount;
//        }

        matchLengths[sequenceCount] = matchLengthBase;

        sequenceCount++;

//        DebugLog.print("Sequence count: %d", sequenceCount);
    }

    public void reset()
    {
        literalsOffset = 0;
        sequenceCount = 0;
        longLengthField = null;

//        start = 0; // TODO for debugging
    }

    public void generateCodes()
    {
        for (int i = 0; i < sequenceCount; ++i) {
            literalLengthCodes[i] = (byte) ZSTD_LLcode(literalLengths[i]);
            offsetCodes[i] = (byte) Util.highestBit(offsets[i]);
            matchLengthCodes[i] = (byte) ZSTD_MLcode(matchLengths[i]);
        }

        if (longLengthField == LongField.LITERAL) {
            literalLengthCodes[longLengthPosition] = Constants.MAX_LITERALS_LENGTH_SYMBOL;
        }
        if (longLengthField == LongField.MATCH) {
            matchLengthCodes[longLengthPosition] = Constants.MAX_MATCH_LENGTH_SYMBOL;
        }
    }

    private static final byte[] LITERAL_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7,
                                                       8, 9, 10, 11, 12, 13, 14, 15,
                                                       16, 16, 17, 17, 18, 18, 19, 19,
                                                       20, 20, 20, 20, 21, 21, 21, 21,
                                                       22, 22, 22, 22, 22, 22, 22, 22,
                                                       23, 23, 23, 23, 23, 23, 23, 23,
                                                       24, 24, 24, 24, 24, 24, 24, 24,
                                                       24, 24, 24, 24, 24, 24, 24, 24};

    // TODO: rename
    private static int ZSTD_LLcode(int literalLength)
    {
        if (literalLength >= 64) {
            return Util.highestBit(literalLength) + 19;
        }
        else {
            return LITERAL_LENGTH_CODE[literalLength];
        }
    }

    private static final byte[] MATCH_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                                     16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                                     32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                                     38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                                     40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                                     41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                                     42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                                     42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42};

    /*
     * note : mlBase = matchLength - MINMATCH;
     *        because it's the format it's stored in seqStore->sequences */
    // TODO: rename
    private static int ZSTD_MLcode(int mlBase)
    {
        if (mlBase >= 128) {
            return Util.highestBit(mlBase) + 36;
        }
        else {
            return MATCH_LENGTH_CODE[mlBase];
        }
    }
}
