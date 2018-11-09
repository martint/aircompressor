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

import static io.airlift.compress.zstd.CompressedBlockState.RepeatMode.REPEAT_NONE;
import static io.airlift.compress.zstd.CompressedBlockState.RepeatMode.REPEAT_VALID;
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
import static io.airlift.compress.zstd.FiniteStateEntropy.optimalTableLog;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;

class SequenceCompressor
{
    private static final int STREAM_ACCUMULATOR_MIN = 57;

    private static final int MAX_LITERALS_LENGTH_SYMBOL = 35;
    private static final int MAX_MATCH_LENGTH_SYMBOL = 52;
    private static final int MAX_SEQUENCES = Math.max(MAX_LITERALS_LENGTH_SYMBOL, MAX_MATCH_LENGTH_SYMBOL);

    private static final int LONG_NUMBER_OF_SEQUENCES = 0x7F00;

    private static final short[] LITERALS_LENGTH_DEFAULT_NORMS = {4, 3, 2, 2, 2, 2, 2, 2,
                                                                  2, 2, 2, 2, 2, 1, 1, 1,
                                                                  2, 2, 2, 2, 2, 2, 2, 2,
                                                                  2, 3, 2, 1, 1, 1, 1, 1,
                                                                  -1, -1, -1, -1};

    private static final int[] LITERALS_LENGTH_BITS = {0, 0, 0, 0, 0, 0, 0, 0,
                                                       0, 0, 0, 0, 0, 0, 0, 0,
                                                       1, 1, 1, 1, 2, 2, 3, 3,
                                                       4, 6, 7, 8, 9, 10, 11, 12,
                                                       13, 14, 15, 16};

    private static final short[] MATCH_LENGTH_DEFAULT_NORMS = {1, 4, 3, 2, 2, 2, 2, 2,
                                                               2, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, 1, 1,
                                                               1, 1, 1, 1, 1, 1, -1, -1,
                                                               -1, -1, -1, -1, -1};

    private static final int[] MATCH_LENGTH_BITS = {0, 0, 0, 0, 0, 0, 0, 0,
                                                    0, 0, 0, 0, 0, 0, 0, 0,
                                                    0, 0, 0, 0, 0, 0, 0, 0,
                                                    0, 0, 0, 0, 0, 0, 0, 0,
                                                    1, 1, 1, 1, 2, 2, 3, 3,
                                                    4, 4, 5, 7, 8, 9, 10, 11,
                                                    12, 13, 14, 15, 16};

    private static final short[] OFFSET_DEFAULT_NORMS = {1, 1, 1, 1, 1, 1, 2, 2,
                                                         2, 1, 1, 1, 1, 1, 1, 1,
                                                         1, 1, 1, 1, 1, 1, 1, 1,
                                                         -1, -1, -1, -1, -1};

    private static final int LITERALS_LENGTH_DEFAULT_NORM_LOG = 6;
    private static final int MATCH_LENGTH_DEFAULT_NORM_LOG = 6;
    private static final int OFFSET_DEFAULT_NORM_LOG = 5;// TODO: repeat is never REPEAT_VALID in current code. It's only set when loading external dictionaries in the native code

    public static int compressSequences(SequenceStore sequences, CompressedBlockState.Entropy previousEntropy, CompressedBlockState.Entropy nextEntropy, CompressionParameters parameters, Object outputBase, final long outputAddress, int outputSize)
    {
        long outputLimit = outputAddress + outputSize;

        CompressionParameters.Strategy strategy = parameters.getStrategy();

        long output = outputAddress;
        int sequenceCount = sequences.sequenceCount;

        // sequences Header
        verify(outputLimit - output > 3 /*max sequenceCount Size*/ + 1 /* flags */, "Output buffer too small");

//        DebugLog.print("Writing %d sequences at offset %d", sequenceCount, output);

        if (sequenceCount < 0x7F) {
            UNSAFE.putByte(outputBase, output, (byte) sequenceCount);
            output++;
        }
        else if (sequenceCount < LONG_NUMBER_OF_SEQUENCES) {
            UNSAFE.putByte(outputBase, output, (byte) (sequenceCount >>> 8 | 0x80));
            UNSAFE.putByte(outputBase, output + 1, (byte) sequenceCount);
            output += SIZE_OF_SHORT;
        }
        else {
            UNSAFE.putByte(outputBase, output, (byte) 0xFF);
            output++;
            UNSAFE.putShort(outputBase, output, (short) (sequenceCount - LONG_NUMBER_OF_SEQUENCES));
            output += SIZE_OF_SHORT;
        }

        if (sequenceCount == 0) {
            /* Copy the old tables over as if we repeated them */
            nextEntropy.copy(previousEntropy);
            return (int) (output - outputAddress);
        }

        // flags for FSE encoding type
        long seqHead = output++;

        // convert length/distances into codes
        sequences.generateCodes();

        // int[] counts = new int[MAX_SEQUENCES + 1];

        long lastNCount = 0;
        int literalsLengthEncodingType;
        int offsetEncodingType;
        int matchLengthEncodingType;

        // literal lengths
        {
//            DebugLog.print("Building LL table");
            int maxTheoreticalSymbol = MAX_LITERALS_LENGTH_SYMBOL;
            int fseLog = LITERALS_LENGTH_FSE_LOG;
            FseCompressionTable previous = previousEntropy.literalLengths.table;
            short[] defaultNorms = LITERALS_LENGTH_DEFAULT_NORMS;
            int defaultNormLog = LITERALS_LENGTH_DEFAULT_NORM_LOG;
            CompressedBlockState.RepeatMode repeatMode = nextEntropy.literalLengths.repeatMode;
            FseCompressionTable next = nextEntropy.literalLengths.table;
            boolean isDefaultAllowed = true;

            Histogram histogram = Histogram.count(sequences.literalLengthCodes, sequenceCount, maxTheoreticalSymbol, new int[MAX_SEQUENCES + 1] /* TODO: preallocate */);
            int maxSymbol = histogram.maxSymbol;
            int largestCount = histogram.largestCount;
            int[] counts = histogram.counts;

            EncodingType encoding = selectEncodingType(repeatMode, counts, maxSymbol, largestCount, sequenceCount, fseLog, previous, defaultNorms, defaultNormLog, isDefaultAllowed, strategy);

//            if (encoding.encoding == SEQUENCE_ENCODING_COMPRESSED) {
//                lastNCount = output;
//            }

            output += buildCompressionTable(
                    outputBase,
                    output,
                    outputSize,
                    encoding.encoding,
                    sequences.literalLengthCodes,
                    counts,
                    previous,
                    fseLog,
                    sequenceCount,
                    maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    maxTheoreticalSymbol,
                    next);

            nextEntropy.literalLengths.repeatMode = encoding.repeatMode;
            literalsLengthEncodingType = encoding.encoding;
        }

        // offset codes
        {
//            DebugLog.print("Building OF table");

            CompressedBlockState.RepeatMode repeatMode = nextEntropy.offsetCodes.repeatMode;
            int fseLog = OFFSET_CODES_FSE_LOG;
            FseCompressionTable previous = previousEntropy.offsetCodes.table;
            short[] defaultNorms = OFFSET_DEFAULT_NORMS;
            int defaultNormLog = OFFSET_DEFAULT_NORM_LOG;
            FseCompressionTable next = nextEntropy.offsetCodes.table;
            int defaultMaxSymbol = DEFAULT_MAX_OFFSET_CODE_SYMBOL; // TODO: figure out the issue of default_max vs max offset code symbol
            int maxTheoreticalSymbol = MAX_OFFSET_CODE_SYMBOL;

            Histogram histogram = Histogram.count(sequences.offsetCodes, sequenceCount, maxTheoreticalSymbol, new int[MAX_SEQUENCES + 1] /* TODO: preallocate */);
            int maxSymbol = histogram.maxSymbol;
            int largestCount = histogram.largestCount;
            int[] counts = histogram.counts;

            /* We can only use the basic table if max <= DEFAULT_MAX_OFFSET_CODE_SYMBOL, otherwise the offsets are too large */
            boolean defaultAllowed = maxSymbol < defaultMaxSymbol;

            EncodingType encoding = selectEncodingType(repeatMode, counts, maxSymbol, largestCount, sequenceCount, fseLog, previous, defaultNorms, defaultNormLog, defaultAllowed, strategy);

//            if (encoding.encoding == SEQUENCE_ENCODING_COMPRESSED) {
//                lastNCount = output;
//            }

            output += buildCompressionTable(
                    outputBase,
                    output,
                    outputSize,
                    encoding.encoding,
                    sequences.offsetCodes,
                    counts,
                    previous,
                    fseLog,
                    sequenceCount,
                    maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    defaultMaxSymbol,
                    next);

            offsetEncodingType = encoding.encoding;
            nextEntropy.offsetCodes.repeatMode = encoding.repeatMode;
        }

        // match lengths
        {
//            DebugLog.print("Building ML table");

            int fseLog = MATCH_LENGTH_FSE_LOG;
            FseCompressionTable previous = previousEntropy.matchLengths.table;
            short[] defaultNorms = MATCH_LENGTH_DEFAULT_NORMS;
            int defaultNormLog = MATCH_LENGTH_DEFAULT_NORM_LOG;
            boolean isDefaultAllowed = true;
            CompressedBlockState.RepeatMode repeateMode = nextEntropy.matchLengths.repeatMode;
            int maxTheoreticalSymbol = MAX_MATCH_LENGTH_SYMBOL;
            FseCompressionTable next = nextEntropy.matchLengths.table;

            Histogram histogram = Histogram.count(sequences.matchLengthCodes, sequenceCount, maxTheoreticalSymbol, new int[MAX_SEQUENCES + 1] /* TODO: preallocate */);
            int maxSymbol = histogram.maxSymbol;
            int largestCount = histogram.largestCount;
            int[] counts = histogram.counts;

            EncodingType encoding = selectEncodingType(repeateMode, counts, maxSymbol, largestCount, sequenceCount, fseLog, previous, defaultNorms, defaultNormLog, isDefaultAllowed, strategy);

//            if (encoding.encoding == SEQUENCE_ENCODING_COMPRESSED) {
//                lastNCount = output;
//            }

            output += buildCompressionTable(
                    outputBase,
                    output,
                    outputSize,
                    encoding.encoding,
                    sequences.matchLengthCodes,
                    counts,
                    previous,
                    fseLog,
                    sequenceCount,
                    maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    maxTheoreticalSymbol,
                    next);

            matchLengthEncodingType = encoding.encoding;
            nextEntropy.matchLengths.repeatMode = encoding.repeatMode;
        }

        // TODO: can avoid having to accumulate encoding types for each section by reading current value from output, doing an | with new value and storing again
        byte encodingTypeHeader = (byte) ((literalsLengthEncodingType << 6) | (offsetEncodingType << 4) | (matchLengthEncodingType << 2));
//        DebugLog.print("Writing FSE encoding type header at offset %d: %x", seqHead, encodingTypeHeader);
        UNSAFE.putByte(outputBase, seqHead, encodingTypeHeader);

        output += encodeSequences(
                outputBase,
                output,
                outputLimit,
                nextEntropy.matchLengths.table,
                nextEntropy.offsetCodes.table,
                nextEntropy.literalLengths.table,
                sequences,
                parameters.getWindowLog() > STREAM_ACCUMULATOR_MIN);

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

        return (int) (output - outputAddress);
    }

    private static int encodeSequences(
            Object outputBase,
            long output,
            long outputLimit,
            FseCompressionTable matchLengthTable,
            FseCompressionTable offsetBitsTable,
            FseCompressionTable literalLengthTable,
            SequenceStore sequences,
            boolean longOffsets)
    {
        byte[] mlCodeTable = sequences.matchLengthCodes;
        byte[] ofCodeTable = sequences.offsetCodes;
        byte[] llCodeTable = sequences.literalLengthCodes;

//        DebugLog.print("Encoding %d sequences at offset %d", sequences.sequenceCount, output);
//        for (int i = 0; i < sequences.sequenceCount; i++) {
//            DebugLog.print("Sequence %d: ll = %d, ml = %d, off = %d", i, sequences.literalLengthCodes[i], sequences.matchLengthCodes[i], sequences.offsetCodes[i]);
//            DebugLog.print("Sequence %d: ll = %d, ml = %d, off = %d", i, sequences.literalLengths[i], sequences.matchLengths[i] + MIN_MATCH, sequences.offsets[i]);
//        }

        BitstreamEncoder blockStream = new BitstreamEncoder(outputBase, output, (int) (outputLimit - output));

        int nbSeq = sequences.sequenceCount;

        // first symbols
//        DebugLog.print("First sequence: ll code: %d, ml code: %d, off code: %d", llCodeTable[nbSeq - 1], mlCodeTable[nbSeq - 1], ofCodeTable[nbSeq - 1]);
        int stateMatchLength = FseCompressor.initialize(matchLengthTable, mlCodeTable[nbSeq - 1]);
        int stateOffsetBits = FseCompressor.initialize(offsetBitsTable, ofCodeTable[nbSeq - 1]);
        int stateLiteralLength = FseCompressor.initialize(literalLengthTable, llCodeTable[nbSeq - 1]);
//        DebugLog.print("First sequence: ll state: %d, ml state: %d, off state: %d", stateLiteralLength, stateMatchLength, stateOffsetBits);

        blockStream.addBits(sequences.literalLengths[nbSeq - 1], LITERALS_LENGTH_BITS[llCodeTable[nbSeq - 1]]);
        blockStream.addBits(sequences.matchLengths[nbSeq - 1], MATCH_LENGTH_BITS[mlCodeTable[nbSeq - 1]]);
        if (longOffsets) {
            // TODO
            throw new UnsupportedOperationException("not yet implemented");
//            U32 const ofBits = ofCodeTable[nbSeq-1];
//            int const extraBits = ofBits - MIN(ofBits, STREAM_ACCUMULATOR_MIN-1);
//            if (extraBits) {
//                BIT_addBits(&blockStream, sequences[nbSeq-1].offset, extraBits);
//                BIT_flushBits(&blockStream);
//            }
//            BIT_addBits(&blockStream, sequences[nbSeq-1].offset >> extraBits, ofBits - extraBits);
        }
        else {
            blockStream.addBits(sequences.offsets[nbSeq - 1], ofCodeTable[nbSeq - 1]);
        }
        blockStream.flush();

        if (nbSeq >= 2) {
            for (int n = nbSeq - 2; n >= 0; n--) {
                byte llCode = llCodeTable[n];
                byte ofCode = ofCodeTable[n];
                byte mlCode = mlCodeTable[n];

                int llBits = LITERALS_LENGTH_BITS[llCode];
                int ofBits = ofCode;
                int mlBits = MATCH_LENGTH_BITS[mlCode];

//                DebugLog.print("encoding: litlen:%2d - matchlen:%2d - offCode:%7d", sequences.literalLengths[n], sequences.matchLengths[n] + MIN_MATCH, sequences.offsets[n]);

                //
                // (7)
                stateOffsetBits = FseCompressor.encode(blockStream, offsetBitsTable, stateOffsetBits, ofCode); // 15
                stateMatchLength = FseCompressor.encode(blockStream, matchLengthTable, stateMatchLength, mlCode); // 24
                stateLiteralLength = FseCompressor.encode(blockStream, literalLengthTable, stateLiteralLength, llCode); // 33

                if ((ofBits + mlBits + llBits >= 64 - 7 - (LITERALS_LENGTH_FSE_LOG + MATCH_LENGTH_FSE_LOG + OFFSET_CODES_FSE_LOG))) {
                    blockStream.flush();                                /* (7)*/
                }

                blockStream.addBits(sequences.literalLengths[n], llBits);
                if (((llBits + mlBits) > 24)) {
                    blockStream.flush();
                }

                blockStream.addBits(sequences.matchLengths[n], mlBits);
                if ((ofBits + mlBits + llBits > 56)) {
                    blockStream.flush();
                }

                if (longOffsets) {
                    // TODO
                    throw new UnsupportedOperationException("not yet implemented");
                    //              int const extraBits = ofBits - MIN(ofBits, STREAM_ACCUMULATOR_MIN-1);
                    //              if (extraBits) {
                    //                  BIT_addBits(&blockStream, sequences[n].offset, extraBits);
                    //                  BIT_flushBits(&blockStream);                            /* (7)*/
                    //              }
                    //              BIT_addBits(&blockStream, sequences[n].offset >> extraBits, ofBits - extraBits);                            /* 31 */
                }
                else {
                    blockStream.addBits(sequences.offsets[n], ofBits); // 31
                }
                blockStream.flush(); // (7)
            }
        }

//        DebugLog.print("flushing ML state with %d bits", matchLengthTable.log2Size);
        FseCompressor.flush(blockStream, matchLengthTable, stateMatchLength);
//        DebugLog.print("flushing OF state with %d bits", offsetBitsTable.log2Size);
        FseCompressor.flush(blockStream, offsetBitsTable, stateOffsetBits);
//        DebugLog.print("flushing LL state with %d bits", literalLengthTable.log2Size);
        FseCompressor.flush(blockStream, literalLengthTable, stateLiteralLength);

        int streamSize = blockStream.close();
        verify(streamSize > 0, "Output buffer too small");

        DebugLog.print("FSE stream size = %d", streamSize);

        return streamSize;
    }

    static class EncodingType
    {
        CompressedBlockState.RepeatMode repeatMode;
        int encoding;

        public EncodingType(int encoding, CompressedBlockState.RepeatMode repeatMode)
        {
            this.repeatMode = repeatMode;
            this.encoding = encoding;
        }
    }

    private static EncodingType selectEncodingType(
            CompressedBlockState.RepeatMode repeatMode,
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
//                DebugLog.print("Selected set_basic");
                return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
            }
//            DebugLog.print("Selected set_rle");
            return new EncodingType(SEQUENCE_ENCODING_RLE, REPEAT_NONE);
        }

        if (strategy.ordinal() < CompressionParameters.Strategy.LAZY.ordinal()) { // TODO: more robust check. Maybe encapsulate in strategy objects
            if (isDefaultAllowed) {
                int staticFse_nbSeq_max = 1000;
                int mult = 10 - strategy.ordinal(); // TODO more robust
                int baseLog = 3;
                long dynamicFse_nbSeq_min = ((1L << defaultNormLog) * mult) >> baseLog;  /* 28-36 for offset, 56-72 for lengths */

                if ((repeatMode == REPEAT_VALID) && (sequenceCount < staticFse_nbSeq_max)) {
//                    DebugLog.print("Selected set_repeat");
                    return new EncodingType(SEQUENCE_ENCODING_REPEAT, REPEAT_VALID);
                }

                if ((sequenceCount < dynamicFse_nbSeq_min) || (largestCount < (sequenceCount >> (defaultNormLog - 1)))) {
                    /* The format allows default tables to be repeated, but it isn't useful.
                     * When using simple heuristics to select encoding type, we don't want
                     * to confuse these tables with dictionaries. When running more careful
                     * analysis, we don't need to waste time checking both repeating tables
                     * and default tables.
                     */
//                    DebugLog.print("Selected set_basic");
                    return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
                }
            }
        }
        else {
            // TODO
            throw new UnsupportedOperationException("not yet implemented");
        }

//        DebugLog.print("Selected set_compressed");
        return new EncodingType(SEQUENCE_ENCODING_COMPRESSED, CompressedBlockState.RepeatMode.REPEAT_CHECK);
    }

    private static int buildCompressionTable(Object outputBase, long outputAddress, int outputSize, int encodingType, byte[] codeTable, int[] counts, FseCompressionTable previous, int fseLog, int numberOfSequences, int maxSymbol, short[] defaultNorm, int defaultNormLog, int defaultMax, FseCompressionTable nextCompressionTable)
    {
        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

//        DebugLog.print("Writing encoding type header at offset %d: %d", output, encodingType);

        switch (encodingType) {
            case SEQUENCE_ENCODING_RLE:
                UNSAFE.putByte(outputBase, output, codeTable[0]);
                FseCompressionTable.makeRleTable(nextCompressionTable, maxSymbol);
                return 1;
            case SEQUENCE_ENCODING_REPEAT:
                throw new UnsupportedOperationException("not yet implemented");
                // TODO: copy previous -> next
//                return 0;
            case SEQUENCE_ENCODING_BASIC:
                FiniteStateEntropy.buildCompressionTable(nextCompressionTable, defaultNorm, defaultMax, defaultNormLog);
                return 0;
            case SEQUENCE_ENCODING_COMPRESSED:
                short[] norm = new short[MAX_SEQUENCES + 1]; // TODO: allocate in context
                int nbSeq_1 = numberOfSequences;

                int tableLog = optimalTableLog(fseLog, numberOfSequences, maxSymbol);
                if (counts[codeTable[numberOfSequences - 1]] > 1) {
                    counts[codeTable[numberOfSequences - 1]]--;
                    nbSeq_1--;
                }

                FiniteStateEntropy.normalizeCounts(norm, tableLog, counts, nbSeq_1, maxSymbol);

                int size = FiniteStateEntropy.writeNormalizedCounts(outputBase, output, (int) (outputLimit - output), norm, maxSymbol, tableLog); // TODO: pass outputLimit directly
                FiniteStateEntropy.buildCompressionTable(nextCompressionTable, norm, maxSymbol, tableLog);

//                DebugLog.print("Writing FSE table definition at offset %d, size = %d", output, size);
                return size;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }
}
