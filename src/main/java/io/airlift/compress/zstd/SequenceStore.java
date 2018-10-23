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

import static io.airlift.compress.zstd.DoubleFastBlockCompressor.MIN_MATCH;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

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
        if (start == 0) {
            start = literalAddress;
        }
        long index = literalAddress - start;
        DebugLog.print("Cpos%7d :%3d literals, match%4d bytes at offCode%7d", index, literalLength, matchLengthBase + MIN_MATCH, offsetCode);

        // TODO: ZSTD_wildcopy
        UNSAFE.copyMemory(literalBase, literalAddress, literalsBuffer, ARRAY_BYTE_BASE_OFFSET + literalsOffset, literalLength);
        literalsOffset += literalLength;

        /* literal Length */
// TODO
//        if (litLength>0xFFFF) {
//            assert(seqStorePtr->longLengthID == 0); /* there can only be a single long length */
//            seqStorePtr->longLengthID = 1;
//            seqStorePtr->longLengthPos = (U32)(seqStorePtr->sequences - seqStorePtr->sequencesStart);
//        }
        literalLengths[sequenceCount] = literalLength;

        /* match offset */
        offsets[sequenceCount] = offsetCode + 1;

// TODO
//        /* match Length */
//        if (mlBase>0xFFFF) {
//            assert(seqStorePtr->longLengthID == 0); /* there can only be a single long length */
//            seqStorePtr->longLengthID = 2;
//            seqStorePtr->longLengthPos = (U32)(seqStorePtr->sequences - seqStorePtr->sequencesStart);
//        }
        matchLengths[sequenceCount] = matchLengthBase;

        sequenceCount++;
    }

    public void reset()
    {
        literalsOffset = 0;
        sequenceCount = 0;
        // TODO: longLengthID

        start = 0; // TODO for debugging
    }

    public void generateCodes()
    {
        for (int i = 0; i < sequenceCount; ++i) {
            literalLengthCodes[i] = (byte) ZSTD_LLcode(literalLengths[i]);
            offsetCodes[i] = (byte) Util.highestBit(offsets[i]);
            matchLengthCodes[i] = (byte) ZSTD_MLcode(matchLengths[i]);
        }

        // TODO
//        if (seqStorePtr->longLengthID==1)
//            llCodeTable[seqStorePtr->longLengthPos] = MaxLL;
//        if (seqStorePtr->longLengthID==2)
//            mlCodeTable[seqStorePtr->longLengthPos] = MaxML;
    }

    private static final byte[] LL_Code = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 15,
                                           16, 16, 17, 17, 18, 18, 19, 19,
                                           20, 20, 20, 20, 21, 21, 21, 21,
                                           22, 22, 22, 22, 22, 22, 22, 22,
                                           23, 23, 23, 23, 23, 23, 23, 23,
                                           24, 24, 24, 24, 24, 24, 24, 24,
                                           24, 24, 24, 24, 24, 24, 24, 24};

    private static int ZSTD_LLcode(int literalLength)
    {
        int LL_deltaCode = 19;
        if (literalLength > 63) {
            return Util.highestBit(literalLength) + LL_deltaCode;
        }
        else {
            return LL_Code[literalLength];
        }
    }

    private static final byte[] ML_Code = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
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
    private static int ZSTD_MLcode(int mlBase)
    {
        int ML_deltaCode = 36;
        if (mlBase > 127) {
            return Util.highestBit(mlBase) + ML_deltaCode;
        }
        else {
            return ML_Code[mlBase];
        }
    }
}
