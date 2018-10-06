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

import static io.airlift.compress.zstd.Constants.DEFAULT_MAX_OFFSET_CODE_SYMBOL;
import static io.airlift.compress.zstd.Constants.LITERALS_LENGTH_FSE_LOG;
import static io.airlift.compress.zstd.Constants.MATCH_LENGTH_FSE_LOG;
import static io.airlift.compress.zstd.Constants.MAX_OFFSET_CODE_SYMBOL;
import static io.airlift.compress.zstd.Constants.OFFSET_CODES_FSE_LOG;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_BASIC;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_COMPRESSED;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_REPEAT;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_RLE;
import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.FseTableReader.FSE_MAX_SYMBOL_VALUE;
import static io.airlift.compress.zstd.SequenceCompressor.EncodingType.REPEAT_CHECK;
import static io.airlift.compress.zstd.SequenceCompressor.EncodingType.REPEAT_NONE;
import static io.airlift.compress.zstd.SequenceCompressor.EncodingType.REPEAT_VALID;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class SequenceCompressor
{
    private static final int STREAM_ACCUMULATOR_MIN = 57;

    private static final int MAX_LITERALS_LENGTH_SYMBOL = 35;
    private static final int MAX_MATCH_LENGTH_SYMBOL = 52;
    private static final int MAX_SEQUENCES = Math.max(MAX_LITERALS_LENGTH_SYMBOL, MAX_MATCH_LENGTH_SYMBOL);

    private static final int LONGNBSEQ = 0x7F00;

    private static final short[] LITERALS_LENGTH_DEFAULT_NORMS = {4, 3, 2, 2, 2, 2, 2, 2,
                                                                  2, 2, 2, 2, 2, 1, 1, 1,
                                                                  2, 2, 2, 2, 2, 2, 2, 2,
                                                                  2, 3, 2, 1, 1, 1, 1, 1,
                                                                  -1, -1, -1, -1};

    private static final short[] MATCH_LENGTH_DEFAULT_NORMS = {1, 4, 3, 2, 2, 2, 2, 2,
                                                               2, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, -1, -1,
                                                               -1, -1, -1, -1, -1};

    private static final short[] OFFSET_DEFAULT_NORMS = {1, 1, 1, 1, 1, 1, 2, 2,
                                                         2, 1, 1, 1, 1, 1, 1, 1,
                                                         1, 1, 1, 1, 1, 1, 1, 1,
                                                         -1, -1, -1, -1, -1};

    private static final int LITERALS_LENGTH_DEFAULT_NORM_LOG = 6;
    private static final int MATCH_LENGTH_DEFAULT_NORM_LOG = 6;
    private static final int OFFSET_DEFAULT_NORM_LOG = 5;

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
        long output = ostart;
        int nbSeq = sequences.sequenceCount;

        /* Compress literals */
        byte[] literals = sequences.literalsBuffer;
        int litSize = sequences.literalsOffset;

        boolean disableLiteralCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);

        int cSize = compressLiterals(previousEntropy.huffman, nextEntropy.huffman, parameters.getStrategy(), disableLiteralCompression, outputBase, output, outputSize, literals, litSize, workspace);

//            if (ZSTD_isError(cSize)) {
//                return cSize;
//            }
        output += cSize;

        /* Sequences Header */
        verify(oend - output > 3 /*max nbSeq Size*/ + 1 /*seqHead*/, outputAddress, "Output buffer too small");

        if (nbSeq < 0x7F) {
//            *op++ = (BYTE)nbSeq;
            UNSAFE.putByte(outputBase, output, (byte) nbSeq);
            output++;
        }
        else if (nbSeq < LONGNBSEQ) {
//            op[0] = (BYTE)((nbSeq>>8) + 0x80), op[1] = (BYTE)nbSeq, op+=2;
            UNSAFE.putByte(outputBase, output, (byte) (nbSeq >>> 8 | 0x80));
            UNSAFE.putByte(outputBase, output, (byte) nbSeq);
            output += SIZE_OF_SHORT;
        }
        else {
//            op[0]=0xFF, MEM_writeLE16(op+1, (U16)(nbSeq - LONGNBSEQ)), op+=3;
            UNSAFE.putByte(outputBase, output, (byte) 0xFF);
            output++;
            UNSAFE.putShort(outputBase, output, (short) (nbSeq - LONGNBSEQ));
            output += SIZE_OF_SHORT;
        }

        if (nbSeq == 0) {
            /* Copy the old tables over as if we repeated them */
//            memcpy(&nextEntropy->fse, &prevEntropy->fse, sizeof(prevEntropy->fse));
            // TODO copy previous->next FSE
            return (int) (output - ostart);
        }

        /* seqHead : flags for FSE encoding type */
        long seqHead = output++;

        /* convert length/distances into codes */
        sequences.generateCodes();

//        int[] counts = new int[MAX_SEQUENCES + 1];

        long lastNCount = 0;

        Histogram histogram;
        int maxSymbol;
        int largestCount;
        int[] counts;

        int countSize = 0;

        /* build CTable for Literal Lengths */
        histogram = count(llCodeTable, nbSeq, MAX_LITERALS_LENGTH_SYMBOL);
        maxSymbol = histogram.maxSymbol;
        largestCount = histogram.largestCount;
        counts = histogram.counts;

        nextEntropy.fse.litlength_repeatMode = previousEntropy.fse.litlength_repeatMode;

        EncodingType literalsLengthType = selectEncodingType(nextEntropy.fse.litlength_repeatMode, counts, maxSymbol, largestCount, nbSeq, LITERALS_LENGTH_FSE_LOG, previousEntropy.fse.litlengthCTable, LITERALS_LENGTH_DEFAULT_NORMS, LITERALS_LENGTH_DEFAULT_NORM_LOG, true, strategy);

// TODO        countSize = ZSTD_buildCTable(output, oend - output, CTable_LitLength, LITERALS_LENGTH_FSE_LOG, literalsLengthType.encoding, counts, maxSymbol, llCodeTable, nbSeq, LITERALS_LENGTH_DEFAULT_NORMS, LITERALS_LENGTH_DEFAULT_NORM_LOG, MAX_LITERALS_LENGTH_SYMBOL, previousEntropy.fse.litlengthCTable, sizeof(prevEntropy -> fse.litlengthCTable), workspace, HUF_WORKSPACE_SIZE);

        if (literalsLengthType.encoding == SEQUENCE_ENCODING_COMPRESSED) {
            lastNCount = output;
        }
        output += countSize;


        /* build CTable for Offsets */
        histogram = count(ofCodeTable, nbSeq, MAX_OFFSET_CODE_SYMBOL);
        maxSymbol = histogram.maxSymbol;
        largestCount = histogram.largestCount;
        counts = histogram.counts;

        /* We can only use the basic table if max <= DEFAULT_MAX_OFFSET_CODE_SYMBOL, otherwise the offsets are too large */
        boolean defaultAllowed = maxSymbol < DEFAULT_MAX_OFFSET_CODE_SYMBOL;
        nextEntropy.fse.offcode_repeatMode = previousEntropy.fse.offcode_repeatMode;

        EncodingType offsetEncodingType = selectEncodingType(nextEntropy.fse.offcode_repeatMode, counts, maxSymbol, largestCount, nbSeq, OFFSET_CODES_FSE_LOG, previousEntropy.fse.offcodeCTable, OFFSET_DEFAULT_NORMS, OFFSET_DEFAULT_NORM_LOG, defaultAllowed, strategy);

//        countSize = ZSTD_buildCTable(output, oend - output, CTable_OffsetBits, OFFSET_CODES_FSE_LOG, offsetEncodingType.encoding, counts, maxSymbol, ofCodeTable, nbSeq, OFFSET_DEFAULT_NORMS, OFFSET_DEFAULT_NORM_LOG, DEFAULT_MAX_OFFSET_CODE_SYMBOL, prevEntropy -> fse.offcodeCTable, sizeof(prevEntropy -> fse.offcodeCTable), workspace, HUF_WORKSPACE_SIZE);

        if (offsetEncodingType.encoding == SEQUENCE_ENCODING_COMPRESSED) {
            lastNCount = output;
        }
        output += countSize;

        /* build CTable for MatchLengths */
        histogram = count(mlCodeTable, nbSeq, MAX_MATCH_LENGTH_SYMBOL);   /* can't fail */
        maxSymbol = histogram.maxSymbol;
        largestCount = histogram.largestCount;
        counts = histogram.counts;

        nextEntropy.fse.matchlength_repeatMode = previousEntropy.fse.matchlength_repeatMode;
        EncodingType matchLengthEncodingType = selectEncodingType(nextEntropy.fse.matchlength_repeatMode, counts, maxSymbol, largestCount, nbSeq, MATCH_LENGTH_FSE_LOG, previousEntropy.fse.matchlengthCTable, MATCH_LENGTH_DEFAULT_NORMS, MATCH_LENGTH_DEFAULT_NORM_LOG, true, strategy);

//        countSize = ZSTD_buildCTable(output, oend - output, CTable_MatchLength, MATCH_LENGTH_FSE_LOG, matchLengthEncodingType.encoding, counts, maxSymbol, mlCodeTable, nbSeq, MATCH_LENGTH_DEFAULT_NORMS, MATCH_LENGTH_DEFAULT_NORM_LOG, MAX_MATCH_LENGTH_SYMBOL, prevEntropy->fse.matchlengthCTable, sizeof(prevEntropy->fse.matchlengthCTable), workspace, HUF_WORKSPACE_SIZE);

        if (matchLengthEncodingType.encoding == SEQUENCE_ENCODING_COMPRESSED) {
            lastNCount = output;
        }
        output += countSize;

        UNSAFE.putByte(outputBase, seqHead, (byte) ((literalsLengthType.encoding << 6) | (offsetEncodingType.encoding << 4) | (matchLengthEncodingType.encoding << 2)));

//            size_t const bitstreamSize = ZSTD_encodeSequences(
//                op, oend - op,
//                CTable_MatchLength, mlCodeTable,
//                CTable_OffsetBits, ofCodeTable,
//                CTable_LitLength, llCodeTable,
//                sequences, nbSeq,
//                longOffsets, bmi2);

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

        return (int) (output - ostart);
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
// TODO: alternate implementation -- benchmark
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
            UNSAFE.putByte(outputBase, outputAddress, (byte) (SEQUENCE_ENCODING_BASIC | (inputSize << 3)));
        }
        else if (inputSize < 4096) {
            headerSize = 2;
            // We're guaranteed to be able to write a short because of the above check w/ inputSize between [32, 4095]
            UNSAFE.putShort(outputBase, outputAddress, (short) (SEQUENCE_ENCODING_BASIC | (1 << 2) | (inputSize << 4)));
        }
        else {
            headerSize = 3;
            // We're guaranteed to be able to write an int because of the above check w/ inputSize >= 4096
            UNSAFE.putInt(outputBase, outputAddress, SEQUENCE_ENCODING_BASIC | (3 << 2) | (inputSize << 4));
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

    static class EncodingType
    {
        public static final int REPEAT_NONE = 0;   /* Cannot use the previous table */
        public static final int REPEAT_CHECK = 1;  /* Can use the previous table but it must be checked */
        public static final int REPEAT_VALID = 2;  /* Can use the previous table and it is asumed to be valid */

        int repeatMode;
        int encoding;

        public EncodingType(int repeatMode, int encoding)
        {
            this.repeatMode = repeatMode;
            this.encoding = encoding;
        }
    }

    private EncodingType selectEncodingType(
            int repeatMode,
            int[] counts,
            int maxSymbol,
            int largestCount,
            int sequenceCount,
            int FSELog,
            FseCompressionTable prevCTable,
            short[] defaultNorm,
            int defaultNormLog,
            boolean isDefaultAllowed,
            CompressionParameters.Strategy strategy)
    {
        if (largestCount == sequenceCount) { // => all entries are equal
            if (isDefaultAllowed && sequenceCount <= 2) {
                /* Prefer set_basic over set_rle when there are 2 or fewer symbols,
                 * since RLE uses 1 byte, but set_basic uses 5-6 bits per symbol.
                 * If basic encoding isn't possible, always choose RLE.
                 */
                return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
            }
            return new EncodingType(SEQUENCE_ENCODING_RLE, REPEAT_NONE);
        }

        if (strategy.ordinal() < CompressionParameters.Strategy.LAZY.ordinal()) { // TODO: more robust check. Maybe encapsulate in strategy objects
            if (isDefaultAllowed) {
                int staticFse_nbSeq_max = 1000;
                int mult = 10 - strategy.ordinal(); // TODO more robust
                int baseLog = 3;
                long dynamicFse_nbSeq_min = ((1L << defaultNormLog) * mult) >> baseLog;  /* 28-36 for offset, 56-72 for lengths */

                if ((repeatMode == REPEAT_VALID) && (sequenceCount < staticFse_nbSeq_max)) {
                    return new EncodingType(SEQUENCE_ENCODING_REPEAT, REPEAT_VALID);
                }

                if ((sequenceCount < dynamicFse_nbSeq_min) || (largestCount < (sequenceCount >> (defaultNormLog - 1)))) {
                    /* The format allows default tables to be repeated, but it isn't useful.
                     * When using simple heuristics to select encoding type, we don't want
                     * to confuse these tables with dictionaries. When running more careful
                     * analysis, we don't need to waste time checking both repeating tables
                     * and default tables.
                     */
                    return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
                }
            }
        }
        else {
            // TODO
            throw new UnsupportedOperationException("not yet implemented");
        }

        return new EncodingType(SEQUENCE_ENCODING_COMPRESSED, REPEAT_CHECK);
    }

//    MEM_STATIC size_t
//    ZSTD_buildCTable(void* dst, size_t dstCapacity, FSE_CTable* nextCTable, U32 FSELog, symbolEncodingType_e type, U32* count, U32 max, const BYTE* codeTable, size_t nbSeq, const S16* defaultNorm, U32 defaultNormLog, U32 defaultMax, const FSE_CTable* prevCTable, size_t prevCTableSize, void* workspace, size_t workspaceSize)
//    {
//        BYTE* op = (BYTE*)dst;
//    const BYTE* const oend = op + dstCapacity;
//
//        switch (type) {
//            case set_rle:
//                *op = codeTable[0];
//                CHECK_F(FSE_buildCTable_rle(nextCTable, (BYTE)max));
//                return 1;
//            case set_repeat:
//                memcpy(nextCTable, prevCTable, prevCTableSize);
//                return 0;
//            case set_basic:
//                CHECK_F(FSE_buildCTable_wksp(nextCTable, defaultNorm, defaultMax, defaultNormLog, workspace, workspaceSize));  /* note : could be pre-calculated */
//                return 0;
//            case set_compressed: {
//                S16 norm[MaxSeq + 1];
//                size_t nbSeq_1 = nbSeq;
//        const U32 tableLog = FSE_optimalTableLog(FSELog, nbSeq, max);
//                if (count[codeTable[nbSeq-1]] > 1) {
//                    count[codeTable[nbSeq-1]]--;
//                    nbSeq_1--;
//                }
//                assert(nbSeq_1 > 1);
//                CHECK_F(FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max));
//                {   size_t const NCountSize = FSE_writeNCount(op, oend - op, norm, max, tableLog);   /* overflow protected */
//                    if (FSE_isError(NCountSize)) return NCountSize;
//                    CHECK_F(FSE_buildCTable_wksp(nextCTable, norm, max, tableLog, workspace, workspaceSize));
//                    return NCountSize;
//                }
//            }
//            default: return assert(0), ERROR(GENERIC);
//        }
//    }

    private static int fseTableStep(int tableSize)
    {
        return (tableSize >>> 1) + (tableSize >>> 3) + 3;
    }

    //    size_t FSE_buildCTable_wksp(FSE_CTable* ct, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog, void* workSpace, size_t wkspSize)
    public static FseCompressionTable buildFseTable(short[] normalizedCounter, int maxSymbolValue, int tableLog)
    {
//        assert(tableLog < 16);   /* required for the threshold strategy to work */
        int tableSize = 1 << tableLog;
        int tableMask = tableSize - 1;

        int step = fseTableStep(tableSize);

//        verify(1 << tableLog <= tableSymbol.length, 0, "Table log too large");
        byte[] tableSymbol = new byte[tableSize]; // TODO: allocate per compressor (see wkspSize)
        int highThreshold = tableSize - 1;

        FseCompressionTable table = new FseCompressionTable(tableLog, maxSymbolValue);

        /* For explanations on how to distribute symbol values over the table :
         *  http://fastcompression.blogspot.fr/2014/02/fse-distributing-symbol-values.html */

        // symbol start positions
        int[] cumulative = new int[FSE_MAX_SYMBOL_VALUE + 2];
        cumulative[0] = 0;
        for (int i = 1; i <= maxSymbolValue + 1; i++) {
            if (normalizedCounter[i - 1] == -1) {  /* Low proba symbol */
                cumulative[i] = cumulative[i - 1] + 1;
                tableSymbol[highThreshold--] = (byte) (i - 1);
            }
            else {
                cumulative[i] = cumulative[i - 1] + normalizedCounter[i - 1];
            }
        }
        cumulative[maxSymbolValue + 1] = tableSize + 1;

        // Spread symbols
        int position = 0;
        for (int symbol = 0; symbol < maxSymbolValue; ++symbol) {
            for (int occurrenceCount = 0; occurrenceCount < normalizedCounter[symbol]; occurrenceCount++) {
                tableSymbol[position] = (byte) symbol;
                position = (position + step) & tableMask;
                while (position > highThreshold) {
                    position = (position + step) & tableMask;   /* Low proba area */
                }
            }
        }

        if (position != 0) { // TODO
            throw new IllegalStateException();   /* Must have gone through all positions */
        }

        // Build table
        for (int i = 0; i < tableSize; i++) {
            byte symbol = tableSymbol[i];
            table.table[cumulative[symbol]++] = (short) (tableSize + i);  /* TableU16 : sorted by symbol order; gives next state value */
        }

        // Build Symbol Transformation Table
        int total = 0;
        for (int symbol = 0; symbol <= maxSymbolValue; symbol++) {
            switch (normalizedCounter[symbol]) {
                case 0:
                    /* filling nonetheless, for compatibility with FSE_getMaxNbBits() */
                    table.deltaNumberOfBits[symbol] = ((tableLog + 1) << 16) - (1 << tableLog);
                    break;
                case -1:
                case 1:
                    table.deltaNumberOfBits[symbol] = (tableLog << 16) - (1 << tableLog);
                    table.deltaFindState[symbol] = total - 1;
                    total++;
                    break;
                default:
                    int maxBitsOut = tableLog - Util.highestBit(normalizedCounter[symbol] - 1);
                    int minStatePlus = normalizedCounter[symbol] << maxBitsOut;
                    table.deltaNumberOfBits[symbol] = (maxBitsOut << 16) - minStatePlus;
                    table.deltaFindState[symbol] = total - normalizedCounter[symbol];
                    total += normalizedCounter[symbol];
                    break;
            }
        }

        return table;
    }

    public static void main(String[] args)
    {
        FseCompressionTable table = buildFseTable(LITERALS_LENGTH_DEFAULT_NORMS, MAX_LITERALS_LENGTH_SYMBOL, LITERALS_LENGTH_DEFAULT_NORM_LOG);

        byte[] input = new byte[100];
        for (int i = 0; i < input.length; i++) {
            input[i] = (byte) (i % 10);
        }

        byte[] output = new byte[100];

        int size = FseCompressor.compress(output, 16, output.length, input, 16, input.length, table);

        new FiniteStateEntropy(LITERALS_LENGTH_FSE_LOG
        System.out.println(size);
    }
}
