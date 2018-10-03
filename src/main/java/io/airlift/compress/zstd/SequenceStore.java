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


public class SequenceStore
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
        sequenceCount = 0;

        literalLengthCodes = new byte[maxSequences];
        matchLengthCodes = new byte[maxSequences];
        offsetCodes = new byte[maxSequences];

        literalsBuffer = new byte[blockSize];
        literalsOffset = 0;
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
        System.out.printf("Cpos%7d :%3d literals, match%4d bytes at offCode%7d\n", index, literalLength, matchLengthBase + MIN_MATCH, offsetCode);


        // TODO: ZSTD_wildcopy
        UNSAFE.copyMemory(literalBase, literalAddress, literalsBuffer,ARRAY_BYTE_BASE_OFFSET + literalsOffset, literalLength);
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
}
