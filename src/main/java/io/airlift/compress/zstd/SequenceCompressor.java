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

import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SequenceCompressor
{
    // TODO: also in ZstdFrameDecompressor
    private static final int SET_BASIC = 0;
    private static final int SET_RLE = 1;
    private static final int SET_COMPRESSED = 2;
    private static final int SET_REPEAT = 3;

    private static final int STREAM_ACCUMULATOR_MIN = 57;

    private static final int MAX_LITERALS_LENGTH_SYMBOL = 35;
    private static final int MAX_MATCH_LENGTH_SYMBOL = 52;
    private static final int MAX_SEQUENCES = Math.max(MAX_LITERALS_LENGTH_SYMBOL, MAX_MATCH_LENGTH_SYMBOL);

    private static final int LONGNBSEQ = 0x7F00;

    //    MEM_STATIC size_t ZSTD_compressSequences_internal(seqStore_t* seqStorePtr, ZSTD_entropyCTables_t const* prevEntropy, ZSTD_entropyCTables_t* nextEntropy, ZSTD_CCtx_params const* cctxParams, void* dst, size_t dstCapacity, U32* workspace, const int bmi2)
    public int compress(SequenceStore sequences, CompressedBlockState.Entropy previousEntropy, CompressedBlockState.Entropy nextEntropy, CompressionParameters parameters, Object outputBase, long outputAddress, int outputSize, int[] workspace)
    {
        int longOffsets = parameters.getWindowLog() > STREAM_ACCUMULATOR_MIN ? 1 : 0;

        CompressionParameters.Strategy strategy = parameters.getStrategy();

//        FSE_CTable* CTable_LitLength = nextEntropy->fse.litlengthCTable;
//        FSE_CTable* CTable_OffsetBits = nextEntropy->fse.offcodeCTable;
//        FSE_CTable* CTable_MatchLength = nextEntropy->fse.matchlengthCTable;

//        const seqDef* const sequences = seqStorePtr->sequencesStart;
        byte[] ofCodeTable = sequences.offsetCodes;
        byte[] llCodeTable = sequences.literalLengthCodes;
        byte[] mlCodeTable = sequences.matchLengthCodes;

        long ostart = outputAddress;
        long oend = ostart + outputSize;
        long op = ostart;
        int nbSeq = sequences.sequenceCount;

        /* Compress literals */
        byte[] literals = sequences.literalsBuffer;
        int litSize = sequences.literalsOffset;

        boolean disableLiteralCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);

        int cSize = compressLiterals(previousEntropy.huffman, nextEntropy.huffman, parameters.getStrategy(), disableLiteralCompression, outputBase, op, outputSize, literals, litSize, workspace);

//            if (ZSTD_isError(cSize)) {
//                return cSize;
//            }
        op += cSize;

        /* Sequences Header */
        verify(oend - op > 3 /*max nbSeq Size*/ + 1 /*seqHead*/, outputAddress, "Output buffer too small");
        
        if (nbSeq < 0x7F) {
//            *op++ = (BYTE)nbSeq;
            UNSAFE.putByte(outputBase, op, (byte) nbSeq);
            op++;
        }
        else if (nbSeq < LONGNBSEQ) {
//            op[0] = (BYTE)((nbSeq>>8) + 0x80), op[1] = (BYTE)nbSeq, op+=2;
            UNSAFE.putByte(outputBase, op, (byte) (nbSeq >>> 8 | 0x80));
            UNSAFE.putByte(outputBase, op, (byte) nbSeq);
            op += SIZE_OF_SHORT;
        }
        else {
//            op[0]=0xFF, MEM_writeLE16(op+1, (U16)(nbSeq - LONGNBSEQ)), op+=3;
            UNSAFE.putByte(outputBase, op, (byte) 0xFF);
            op++;
            UNSAFE.putShort(outputBase, op, (short) (nbSeq - LONGNBSEQ));
            op += SIZE_OF_SHORT;
        }
        
        if (nbSeq==0) {
            /* Copy the old tables over as if we repeated them */
//            memcpy(&nextEntropy->fse, &prevEntropy->fse, sizeof(prevEntropy->fse));
            // TODO copy previous->next FSE
            return (int) (op - ostart);
        }


        int LLtype, Offtype, MLtype;   /* compressed, raw or rle */

        /* seqHead : flags for FSE encoding type */
        long seqHead = op++;

        /* convert length/distances into codes */
        sequences.generateCodes();

//        int[] counts = new int[MAX_SEQUENCES + 1];

        /* build CTable for Literal Lengths */
        Histogram histogram = count(llCodeTable, nbSeq, MAX_LITERALS_LENGTH_SYMBOL);

        int max = histogram.maxSymbol;
        int mostFrequent = histogram.largestCount;
        int[] counts = histogram.counts;

        nextEntropy.fse.litlength_repeatMode = previousEntropy.fse.litlength_repeatMode;

        LLtype = ZSTD_selectEncodingType(&nextEntropy.fse.litlength_repeatMode, counts, max, mostFrequent, nbSeq, LLFSELog, previousEntropy.fse.litlengthCTable, LL_defaultNorm, LL_defaultNormLog, ZSTD_defaultAllowed, strategy);

        int countSize = ZSTD_buildCTable(op, oend - op, CTable_LitLength, LLFSELog, (symbolEncodingType_e)LLtype, counts, max, llCodeTable, nbSeq, LL_defaultNorm, LL_defaultNormLog, MaxLL, previousEntropy.fse.litlengthCTable, sizeof(prevEntropy->fse.litlengthCTable), workspace, HUF_WORKSPACE_SIZE);

        long lastNCount = 0;
        if (LLtype == set_compressed) {
            lastNCount = op;
        }
        op += countSize;



//        /* build CTable for Offsets */
//        {   U32 max = MaxOff;
//            size_t const mostFrequent = HIST_countFast_wksp(count, &max, ofCodeTable, nbSeq, workspace);  /* can't fail */
//            /* We can only use the basic table if max <= DefaultMaxOff, otherwise the offsets are too large */
//            ZSTD_defaultPolicy_e const defaultPolicy = (max <= DefaultMaxOff) ? ZSTD_defaultAllowed : ZSTD_defaultDisallowed;
//            DEBUGLOG(5, "Building OF table");
//            nextEntropy->fse.offcode_repeatMode = prevEntropy->fse.offcode_repeatMode;
//            Offtype = ZSTD_selectEncodingType(&nextEntropy->fse.offcode_repeatMode, count, max, mostFrequent, nbSeq, OffFSELog, prevEntropy->fse.offcodeCTable, OF_defaultNorm, OF_defaultNormLog, defaultPolicy, strategy);
//            assert(!(Offtype < set_compressed && nextEntropy->fse.offcode_repeatMode != FSE_repeat_none)); /* We don't copy tables */
//            {   size_t const countSize = ZSTD_buildCTable(op, oend - op, CTable_OffsetBits, OffFSELog, (symbolEncodingType_e)Offtype,
//                    count, max, ofCodeTable, nbSeq, OF_defaultNorm, OF_defaultNormLog, DefaultMaxOff,
//                    prevEntropy->fse.offcodeCTable, sizeof(prevEntropy->fse.offcodeCTable),
//                    workspace, HUF_WORKSPACE_SIZE);
//                if (ZSTD_isError(countSize)) return countSize;
//                if (Offtype == set_compressed)
//                    lastNCount = op;
//                op += countSize;
//            }   }
//        /* build CTable for MatchLengths */
//        {   U32 max = MaxML;
//            size_t const mostFrequent = HIST_countFast_wksp(count, &max, mlCodeTable, nbSeq, workspace);   /* can't fail */
//            DEBUGLOG(5, "Building ML table");
//            nextEntropy->fse.matchlength_repeatMode = prevEntropy->fse.matchlength_repeatMode;
//            MLtype = ZSTD_selectEncodingType(&nextEntropy->fse.matchlength_repeatMode, count, max, mostFrequent, nbSeq, MLFSELog, prevEntropy->fse.matchlengthCTable, ML_defaultNorm, ML_defaultNormLog, ZSTD_defaultAllowed, strategy);
//            assert(!(MLtype < set_compressed && nextEntropy->fse.matchlength_repeatMode != FSE_repeat_none)); /* We don't copy tables */
//            {   size_t const countSize = ZSTD_buildCTable(op, oend - op, CTable_MatchLength, MLFSELog, (symbolEncodingType_e)MLtype,
//                    count, max, mlCodeTable, nbSeq, ML_defaultNorm, ML_defaultNormLog, MaxML,
//                    prevEntropy->fse.matchlengthCTable, sizeof(prevEntropy->fse.matchlengthCTable),
//                    workspace, HUF_WORKSPACE_SIZE);
//                if (ZSTD_isError(countSize)) return countSize;
//                if (MLtype == set_compressed)
//                    lastNCount = op;
//                op += countSize;
//            }   }
//
//        *seqHead = (BYTE)((LLtype<<6) + (Offtype<<4) + (MLtype<<2));
//
//        {   size_t const bitstreamSize = ZSTD_encodeSequences(
//                op, oend - op,
//                CTable_MatchLength, mlCodeTable,
//                CTable_OffsetBits, ofCodeTable,
//                CTable_LitLength, llCodeTable,
//                sequences, nbSeq,
//                longOffsets, bmi2);
//            if (ZSTD_isError(bitstreamSize)) return bitstreamSize;
//            op += bitstreamSize;
//            /* zstd versions <= 1.3.4 mistakenly report corruption when
//             * FSE_readNCount() recieves a buffer < 4 bytes.
//             * Fixed by https://github.com/facebook/zstd/pull/1146.
//             * This can happen when the last set_compressed table present is 2
//             * bytes and the bitstream is only one byte.
//             * In this exceedingly rare case, we will simply emit an uncompressed
//             * block, since it isn't worth optimizing.
//             */
//            if (lastNCount && (op - lastNCount) < 4) {
//                /* NCountSize >= 2 && bitstreamSize > 0 ==> lastCountSize == 3 */
//                assert(op - lastNCount == 3);
//                DEBUGLOG(5, "Avoiding bug in zstd decoder in versions <= 1.3.4 by "
//                        "emitting an uncompressed block.");
//                return 0;
//            }
//        }
//
        return (int) (op - ostart);
    }

    private int compressLiterals(
            CompressedBlockState.HuffmanTable previousHuffman,
            CompressedBlockState.HuffmanTable nextHuffman,
            CompressionParameters.Strategy strategy,
            boolean disableLiteralCompression,
            Object outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize,
            int[] workspace)
    {
        return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
    }

//    static size_t ZSTD_compressLiterals (ZSTD_hufCTables_t const* prevHuf,
//            ZSTD_hufCTables_t* nextHuf,
//            ZSTD_strategy strategy, int disableLiteralCompression,
//            void* dst, size_t dstCapacity,
//                               const void* src, size_t srcSize,
//            U32* workspace, const int bmi2)
//    {
//        size_t const minGain = ZSTD_minGain(srcSize, strategy);
//        size_t const lhSize = 3 + (srcSize >= 1 KB) + (srcSize >= 16 KB);
//        BYTE*  const ostart = (BYTE*)dst;
//        U32 singleStream = srcSize < 256;
//        symbolEncodingType_e hType = set_compressed;
//        size_t cLitSize;
//
//        DEBUGLOG(5,"ZSTD_compressLiterals (disableLiteralCompression=%i)",
//                disableLiteralCompression);
//
//        /* Prepare nextEntropy assuming reusing the existing table */
//        memcpy(nextHuf, prevHuf, sizeof(*prevHuf));
//
//        if (disableLiteralCompression)
//            return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
//
//        /* small ? don't even attempt compression (speed opt) */
//#   define COMPRESS_LITERALS_SIZE_MIN 63
//        {   size_t const minLitSize = (prevHuf->repeatMode == HUF_repeat_valid) ? 6 : COMPRESS_LITERALS_SIZE_MIN;
//            if (srcSize <= minLitSize) return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
//        }
//
//        if (dstCapacity < lhSize+1) return ERROR(dstSize_tooSmall);   /* not enough space for compression */
//        {   HUF_repeat repeat = prevHuf->repeatMode;
//            int const preferRepeat = strategy < ZSTD_lazy ? srcSize <= 1024 : 0;
//            if (repeat == HUF_repeat_valid && lhSize == 3) singleStream = 1;
//            cLitSize = singleStream ? HUF_compress1X_repeat(ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 11,
//                    workspace, HUF_WORKSPACE_SIZE, (HUF_CElt*)nextHuf->CTable, &repeat, preferRepeat, bmi2)
//                                : HUF_compress4X_repeat(ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 11,
//                workspace, HUF_WORKSPACE_SIZE, (HUF_CElt*)nextHuf->CTable, &repeat, preferRepeat, bmi2);
//            if (repeat != HUF_repeat_none) {
//                /* reused the existing table */
//                hType = set_repeat;
//            }
//        }
//
//        if ((cLitSize==0) | (cLitSize >= srcSize - minGain) | ERR_isError(cLitSize)) {
//            memcpy(nextHuf, prevHuf, sizeof(*prevHuf));
//            return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
//        }
//        if (cLitSize==1) {
//            memcpy(nextHuf, prevHuf, sizeof(*prevHuf));
//            return ZSTD_compressRleLiteralsBlock(dst, dstCapacity, src, srcSize);
//        }
//
//        if (hType == set_compressed) {
//            /* using a newly constructed table */
//            nextHuf->repeatMode = HUF_repeat_check;
//        }
//
//        /* Build header */
//        switch(lhSize)
//        {
//            case 3: /* 2 - 2 - 10 - 10 */
//            {   U32 const lhc = hType + ((!singleStream) << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<14);
//                MEM_writeLE24(ostart, lhc);
//                break;
//            }
//            case 4: /* 2 - 2 - 14 - 14 */
//            {   U32 const lhc = hType + (2 << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<18);
//                MEM_writeLE32(ostart, lhc);
//                break;
//            }
//            case 5: /* 2 - 2 - 18 - 18 */
//            {   U32 const lhc = hType + (3 << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<22);
//                MEM_writeLE32(ostart, lhc);
//                ostart[4] = (BYTE)(cLitSize >> 10);
//                break;
//            }
//            default:  /* not possible : lhSize is {3,4,5} */
//                assert(0);
//        }
//        return lhSize+cLitSize;
//    }

    private int rawLiterals(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize)
    {
//        int headerSize = 1;
//        if (inputSize > 31) {
//            headerSize++;
//        }
//        if (inputSize > 4095) {
//            headerSize++;
//        }
//
//        verify(inputSize + headerSize > outputSize, inputAddress, "Output buffer too small");
//
//        switch (headerSize) {
//            case 1:
//                UNSAFE.putByte(outputBase, outputAddress, (byte) (SET_BASIC | (inputSize << 3)));
//                break;
//            case 2:
//                UNSAFE.putShort(outputBase, outputAddress, (short) (SET_BASIC | (1 << 2) | (inputSize << 4)));
//                break;
//            case 3:
//                UNSAFE.putInt(outputBase, outputAddress, SET_BASIC | (3 << 2) | (inputSize << 4));
//                break;
//            default:
//                throw new AssertionError();
//        }

        verify(outputSize >= inputSize + 1, inputAddress, "Output buffer too small");

        int headerSize;
        if (inputSize < 32) {
            headerSize = 1;
            UNSAFE.putByte(outputBase, outputAddress, (byte) (SET_BASIC | (inputSize << 3)));
        }
        else if (inputSize < 4096) {
            headerSize = 2;
            // We're guaranteed to be able to write a short because of the above check w/ inputSize between [32, 4095]
            UNSAFE.putShort(outputBase, outputAddress, (short) (SET_BASIC | (1 << 2) | (inputSize << 4)));
        }
        else {
            headerSize = 3;
            // We're guaranteed to be able to write an int because of the above check w/ inputSize >= 4096
            UNSAFE.putInt(outputBase, outputAddress, SET_BASIC | (3 << 2) | (inputSize << 4));
        }

        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress + headerSize, inputSize);

        return headerSize + inputSize;
    }


    private static class Histogram
    {
        int maxSymbol;
        int largestCount;
        private final int[] counts;

        public Histogram(int maxSymbol, int largestCount, int[] counts)
        {
            this.maxSymbol = maxSymbol;
            this.largestCount = largestCount;
            this.counts = counts;
        }
    }

    private static Histogram count(byte[] input, int length, int maxSymbol)
    {
        // TODO: HIST_count_parallel_wksp heuristic

        // TODO: allocate once per compressor & fill with 0s:  int[] counts = new int[MAX_SEQUENCES + 1];
        int[] counts = new int[maxSymbol + 1];

        if (length == 0) {
            return new Histogram(0, 0, counts);
        }

        for (int i = 0; i < length; i++) {
            counts[input[i]]++;
        }

        while (counts[maxSymbol] == 0) {
            maxSymbol--;
        }

        int largestCount = 0;
        for (int i = 0; i < maxSymbol; i++) {
            if (counts[i] > largestCount) {
                largestCount = counts[i];
            }
        }

        return new Histogram(maxSymbol, largestCount, counts);
    }

//
//    /* HIST_countFast_wksp() :
//     * Same as HIST_countFast(), but using an externally provided scratch buffer.
//     * `workSpace` size must be table of >= HIST_WKSP_SIZE_U32 unsigned */
//    size_t HIST_countFast_wksp(unsigned* count, unsigned* maxSymbolValuePtr,
//                          const void* source, size_t sourceSize,
//            unsigned* workSpace)
//    {
//        if (sourceSize < 1500) /* heuristic threshold */
//            return HIST_count_simple(count, maxSymbolValuePtr, source, sourceSize);
//        return HIST_count_parallel_wksp(count, maxSymbolValuePtr, source, sourceSize, 0, workSpace);
//    }
//
//    unsigned HIST_count_simple(unsigned* count, unsigned* maxSymbolValuePtr,
//                           const void* src, size_t srcSize)
//    {
//    const BYTE* ip = (const BYTE*)src;
//    const BYTE* const end = ip + srcSize;
//        unsigned maxSymbolValue = *maxSymbolValuePtr;
//        unsigned largestCount=0;
//
//        memset(count, 0, (maxSymbolValue+1) * sizeof(*count));
//        if (srcSize==0) { *maxSymbolValuePtr = 0; return 0; }
//
//        while (ip<end) {
//            assert(*ip <= maxSymbolValue);
//            count[*ip++]++;
//        }
//
//        while (!count[maxSymbolValue]) maxSymbolValue--;
//    *maxSymbolValuePtr = maxSymbolValue;
//
//        {   U32 s;
//            for (s=0; s<=maxSymbolValue; s++)
//                if (count[s] > largestCount) largestCount = count[s];
//        }
//
//        return largestCount;
//    }
//
//    /* HIST_count_parallel_wksp() :
//     * store histogram into 4 intermediate tables, recombined at the end.
//     * this design makes better use of OoO cpus,
//     * and is noticeably faster when some values are heavily repeated.
//     * But it needs some additional workspace for intermediate tables.
//     * `workSpace` size must be a table of size >= HIST_WKSP_SIZE_U32.
//     * @return : largest histogram frequency,
//     *           or an error code (notably when histogram would be larger than *maxSymbolValuePtr). */
//    static size_t HIST_count_parallel_wksp(
//            unsigned* count, unsigned* maxSymbolValuePtr,
//                                const void* source, size_t sourceSize,
//            unsigned checkMax,
//            unsigned* const workSpace)
//    {
//    const BYTE* ip = (const BYTE*)source;
//    const BYTE* const iend = ip+sourceSize;
//        unsigned maxSymbolValue = *maxSymbolValuePtr;
//        unsigned max=0;
//        U32* const Counting1 = workSpace;
//        U32* const Counting2 = Counting1 + 256;
//        U32* const Counting3 = Counting2 + 256;
//        U32* const Counting4 = Counting3 + 256;
//
//        memset(workSpace, 0, 4*256*sizeof(unsigned));
//
//        /* safety checks */
//        if (!sourceSize) {
//            memset(count, 0, maxSymbolValue + 1);
//        *maxSymbolValuePtr = 0;
//            return 0;
//        }
//        if (!maxSymbolValue) maxSymbolValue = 255;            /* 0 == default */
//
//        /* by stripes of 16 bytes */
//        {   U32 cached = MEM_read32(ip); ip += 4;
//            while (ip < iend-15) {
//                U32 c = cached; cached = MEM_read32(ip); ip += 4;
//                Counting1[(BYTE) c     ]++;
//                Counting2[(BYTE)(c>>8) ]++;
//                Counting3[(BYTE)(c>>16)]++;
//                Counting4[       c>>24 ]++;
//                c = cached; cached = MEM_read32(ip); ip += 4;
//                Counting1[(BYTE) c     ]++;
//                Counting2[(BYTE)(c>>8) ]++;
//                Counting3[(BYTE)(c>>16)]++;
//                Counting4[       c>>24 ]++;
//                c = cached; cached = MEM_read32(ip); ip += 4;
//                Counting1[(BYTE) c     ]++;
//                Counting2[(BYTE)(c>>8) ]++;
//                Counting3[(BYTE)(c>>16)]++;
//                Counting4[       c>>24 ]++;
//                c = cached; cached = MEM_read32(ip); ip += 4;
//                Counting1[(BYTE) c     ]++;
//                Counting2[(BYTE)(c>>8) ]++;
//                Counting3[(BYTE)(c>>16)]++;
//                Counting4[       c>>24 ]++;
//            }
//            ip-=4;
//        }
//
//        /* finish last symbols */
//        while (ip<iend) Counting1[*ip++]++;
//
//        if (checkMax) {   /* verify stats will fit into destination table */
//            U32 s; for (s=255; s>maxSymbolValue; s--) {
//                Counting1[s] += Counting2[s] + Counting3[s] + Counting4[s];
//                if (Counting1[s]) return ERROR(maxSymbolValue_tooSmall);
//            }   }
//
//        {   U32 s;
//            if (maxSymbolValue > 255) maxSymbolValue = 255;
//            for (s=0; s<=maxSymbolValue; s++) {
//                count[s] = Counting1[s] + Counting2[s] + Counting3[s] + Counting4[s];
//                if (count[s] > max) max = count[s];
//            }   }
//
//        while (!count[maxSymbolValue]) maxSymbolValue--;
//    *maxSymbolValuePtr = maxSymbolValue;
//        return (size_t)max;
//    }

}
