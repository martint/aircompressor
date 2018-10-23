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
import static io.airlift.compress.zstd.Constants.MIN_MATCH;
import static io.airlift.compress.zstd.Constants.OFFSET_CODES_FSE_LOG;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_BASIC;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_COMPRESSED;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_REPEAT;
import static io.airlift.compress.zstd.Constants.SEQUENCE_ENCODING_RLE;
import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.FiniteStateEntropy.MIN_TABLE_LOG;
import static io.airlift.compress.zstd.FiniteStateEntropy.minTableLog;
import static io.airlift.compress.zstd.FiniteStateEntropy.optimalTableLog;
import static io.airlift.compress.zstd.FseTableReader.FSE_MAX_SYMBOL_VALUE;
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

    private static final int[] LL_bits = {0, 0, 0, 0, 0, 0, 0, 0,
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

    private static final int[] ML_bits = {0, 0, 0, 0, 0, 0, 0, 0,
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
    private static final int OFFSET_DEFAULT_NORM_LOG = 5;

    private static final int[] REST_TO_BEAT = new int[] {0, 473195, 504333, 520860, 550000, 700000, 750000, 830000};
    private static final short UNASSIGNED = -2;

    public static int compress(SequenceStore sequences, CompressedBlockState.Entropy previousEntropy, CompressedBlockState.Entropy nextEntropy, CompressionParameters parameters, Object outputBase, final long outputAddress, int outputSize)
    {
        long outputLimit = outputAddress + outputSize;

        CompressionParameters.Strategy strategy = parameters.getStrategy();

        long output = outputAddress;
        int sequenceCount = sequences.sequenceCount;

        /* Compress literals */
        byte[] literals = sequences.literalsBuffer;
        int litSize = sequences.literalsOffset;

        boolean disableLiteralCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);

        output += compressLiterals(previousEntropy.huffman, nextEntropy.huffman, parameters.getStrategy(), disableLiteralCompression, outputBase, output, outputSize, literals, litSize);

        /* Sequences Header */
        verify(outputLimit - output > 3 /*max sequenceCount Size*/ + 1 /*seqHead*/, outputAddress, "Output buffer too small");

        DebugLog.print("Writing %d sequences at offset %d", sequenceCount, output);
        
        if (sequenceCount < 0x7F) {
            UNSAFE.putByte(outputBase, output, (byte) sequenceCount);
            output++;
        }
        else if (sequenceCount < LONGNBSEQ) {
            UNSAFE.putByte(outputBase, output, (byte) (sequenceCount >>> 8 | 0x80));
            UNSAFE.putByte(outputBase, output + 1, (byte) sequenceCount);
            output += SIZE_OF_SHORT;
        }
        else {
            UNSAFE.putByte(outputBase, output, (byte) 0xFF);
            output++;
            UNSAFE.putShort(outputBase, output, (short) (sequenceCount - LONGNBSEQ));
            output += SIZE_OF_SHORT;
        }

        if (sequenceCount == 0) {
            /* Copy the old tables over as if we repeated them */
            // TODO copy previous->next FSE
            nextEntropy.copy(previousEntropy);
            return (int) (output - outputAddress);
        }

        /* seqHead : flags for FSE encoding type */
        long seqHead = output++;

        /* convert length/distances into codes */
        sequences.generateCodes();

        // int[] counts = new int[MAX_SEQUENCES + 1];

        long lastNCount = 0;
        int literalsLengthEncodingType;
        int offsetEncodingType;
        int matchLengthEncodingType;

        // literal lengths
        {
            DebugLog.print("Building LL table");
            int maxTheoreticalSymbol = MAX_LITERALS_LENGTH_SYMBOL;
            int fseLog = LITERALS_LENGTH_FSE_LOG;
            FseCompressionTable previous = previousEntropy.literalLengths.table;
            short[] defaultNorms = LITERALS_LENGTH_DEFAULT_NORMS;
            int defaultNormLog = LITERALS_LENGTH_DEFAULT_NORM_LOG;
            CompressedBlockState.RepeatMode repeatMode = nextEntropy.literalLengths.repeatMode;
            FseCompressionTable next = nextEntropy.literalLengths.table;
            boolean isDefaultAllowed = true;

            Histogram histogram = count(sequences.literalLengthCodes, sequenceCount, maxTheoreticalSymbol);
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
                    (byte) maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    maxTheoreticalSymbol,
                    next);

            nextEntropy.literalLengths.repeatMode = encoding.repeatMode;
            literalsLengthEncodingType = encoding.encoding;
        }

        // offset codes
        {
            DebugLog.print("Building OF table");

            CompressedBlockState.RepeatMode repeatMode = nextEntropy.offsetCodes.repeatMode;
            int fseLog = OFFSET_CODES_FSE_LOG;
            FseCompressionTable previous = previousEntropy.offsetCodes.table;
            short[] defaultNorms = OFFSET_DEFAULT_NORMS;
            int defaultNormLog = OFFSET_DEFAULT_NORM_LOG;
            FseCompressionTable next = nextEntropy.offsetCodes.table;
            int defaultMaxSymbol = DEFAULT_MAX_OFFSET_CODE_SYMBOL; // TODO: figure out the issue of default_max vs max offset code symbol
            int maxTheoreticalSymbol = MAX_OFFSET_CODE_SYMBOL;

            Histogram histogram = count(sequences.offsetCodes, sequenceCount, maxTheoreticalSymbol);
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
                    (byte) maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    defaultMaxSymbol,
                    next);

            offsetEncodingType = encoding.encoding;
            nextEntropy.offsetCodes.repeatMode = encoding.repeatMode;
        }

        // match lengths
        {
            DebugLog.print("Building ML table");

            int fseLog = MATCH_LENGTH_FSE_LOG;
            FseCompressionTable previous = previousEntropy.matchLengths.table;
            short[] defaultNorms = MATCH_LENGTH_DEFAULT_NORMS;
            int defaultNormLog = MATCH_LENGTH_DEFAULT_NORM_LOG;
            boolean isDefaultAllowed = true;
            CompressedBlockState.RepeatMode repeateMode = nextEntropy.matchLengths.repeatMode;
            int maxTheoreticalSymbol = MAX_MATCH_LENGTH_SYMBOL;
            FseCompressionTable next = nextEntropy.matchLengths.table;

            Histogram histogram = count(sequences.matchLengthCodes, sequenceCount, maxTheoreticalSymbol);   /* can't fail */
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
                    (byte) maxSymbol,
                    defaultNorms,
                    defaultNormLog,
                    maxTheoreticalSymbol,
                    next);

            matchLengthEncodingType = encoding.encoding;
            nextEntropy.matchLengths.repeatMode = encoding.repeatMode;
        }

        // TODO: can avoid having to accumulate encoding types for each section by reading current value from output, doing an | with new value and storing again
        byte encodingTypeHeader = (byte) ((literalsLengthEncodingType << 6) | (offsetEncodingType << 4) | (matchLengthEncodingType << 2));
        DebugLog.print("Writing FSE encoding type header at offset %d: %x", seqHead, encodingTypeHeader);
        UNSAFE.putByte(outputBase, seqHead, encodingTypeHeader);

        output += encodeSequences(
                outputBase,
                output,
                outputLimit - output,
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

    //    size_t FSE_writeNCount (void* buffer, size_t bufferSize, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
    public static int writeNormalizedCounts(Object outputBase, long outputAddress, int outputSize, short[] normalizedCounts, int maxSymbol, int tableLog)
    {
        verify(tableLog <= FiniteStateEntropy.MAX_TABLE_LOG, "FSE table too large");
        verify(tableLog >= FiniteStateEntropy.MIN_TABLE_LOG, "FSE table too small");

        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        int tableSize = 1 << tableLog;

        int bitCount = 0;

        // encode table size
        int bitStream = (tableLog - MIN_TABLE_LOG);
        bitCount += 4;

        int remaining = tableSize + 1; // +1 for extra accuracy
        int threshold = tableSize;
        int nbBits = tableLog + 1;

        int symbol = 0;

        boolean previous0 = false;
        while (remaining > 1) {
            if (previous0) {
                // From RFC 8478:
                //   When a symbol has a probability of zero, it is followed by a 2-bit
                //   repeat flag.  This repeat flag tells how many probabilities of zeroes
                //   follow the current one.  It provides a number ranging from 0 to 3.
                //   If it is a 3, another 2-bit repeat flag follows, and so on.
                int start = symbol;

                // find run of symbols with count 0
                while (normalizedCounts[symbol] == 0) {
                    symbol++;
                }

                // encode in batches if 8 repeat sequences in one shot (representing 24 symbols total)
                while (symbol >= start + 24) {
                    start += 24;
                    bitStream |= (0b11_11_11_11_11_11_11_11 << bitCount);
                    verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                    // TODO: putShort?
                    UNSAFE.putByte(outputBase, output, (byte) bitStream);
                    UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                    output += SIZE_OF_SHORT;

                    // flush now, so no need to increase bitCount by 16
                    bitStream >>>= Short.SIZE;
                }

                // encode remaining in batches of 3 symbols
                while (symbol >= start + 3) {
                    start += 3;
                    bitStream += 0b11 << bitCount;
                    bitCount += 2;
                }

                // encode tail
                bitStream += (symbol - start) << bitCount;
                bitCount += 2;

                // flush bitstream if necessary
                if (bitCount > 16) {
                    verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                    // TODO: putShort?
                    UNSAFE.putByte(outputBase, output, (byte) bitStream);
                    UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                    output += SIZE_OF_SHORT;

                    bitStream >>= Short.SIZE;
                    bitCount -= Short.SIZE;
                }
            }

            int count = normalizedCounts[symbol++];
            int max = (2 * threshold - 1) - remaining;
            remaining -= count < 0 ? -count : count;
            count++;   /* +1 for extra accuracy */
            if (count >= threshold) {
                count += max;
            }
            bitStream += count << bitCount;
            bitCount += nbBits;
            bitCount -= (count < max ? 1 : 0);
            previous0 = (count == 1);

            verify(remaining >= 1, "Error"); // TODO

            while (remaining < threshold) {
                nbBits--;
                threshold >>= 1;
            }

            // flush bitstream if necessary
            if (bitCount > 16) {
                verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");

                // TODO: putShort?
                UNSAFE.putByte(outputBase, output, (byte) bitStream);
                UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
                output += SIZE_OF_SHORT;

                bitStream >>= Short.SIZE;
                bitCount -= Short.SIZE;
            }
        }

        // flush remaining bitstream
        verify(output + SIZE_OF_SHORT <= outputLimit, "Output buffer too small");
        // TODO: putShort?
        UNSAFE.putByte(outputBase, output, (byte) bitStream);
        UNSAFE.putByte(outputBase, output + 1, (byte) (bitStream >>> 8));
        output += (bitCount + 7) / 8;

        verify(symbol <= maxSymbol + 1, "Error"); // TODO

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

        DebugLog.print("Encoding %d sequences at offset %d", sequences.sequenceCount, output);
        for (int i = 0; i < sequences.sequenceCount; i++) {
            DebugLog.print("Sequence %d: ll = %d, ml = %d, off = %d", i, sequences.literalLengths[i], sequences.matchLengths[i] + MIN_MATCH, sequences.offsets[i]);
        }
        
        BitstreamEncoder blockStream = new BitstreamEncoder(outputBase, output, (int) (outputLimit - output));

        int nbSeq = sequences.sequenceCount;

        // first symbols
        DebugLog.print("First sequence: ll code: %d, ml code: %d, off code: %d", llCodeTable[nbSeq - 1], mlCodeTable[nbSeq - 1], ofCodeTable[nbSeq - 1]);
        int stateMatchLength = FseCompressor.initialize(matchLengthTable, mlCodeTable[nbSeq - 1]);
        int stateOffsetBits = FseCompressor.initialize(offsetBitsTable, ofCodeTable[nbSeq - 1]);
        int stateLiteralLength = FseCompressor.initialize(literalLengthTable, llCodeTable[nbSeq - 1]);
        DebugLog.print("First sequence: ll state: %d, ml state: %d, off state: %d", stateLiteralLength, stateMatchLength, stateOffsetBits);

        blockStream.addBits(sequences.literalLengths[nbSeq - 1], LL_bits[llCodeTable[nbSeq - 1]]);
        blockStream.addBits(sequences.matchLengths[nbSeq - 1], ML_bits[mlCodeTable[nbSeq - 1]]);
        if (longOffsets) {
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

                int llBits = LL_bits[llCode];
                int ofBits = ofCode;
                int mlBits = ML_bits[mlCode];

                DebugLog.print("encoding: litlen:%2d - matchlen:%2d - offCode:%7d",
                        sequences.literalLengths[n],
                        sequences.matchLengths[n] + MIN_MATCH,
                        sequences.offsets[n]);

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

        FseCompressor.flush(blockStream, matchLengthTable, stateMatchLength);
        FseCompressor.flush(blockStream, offsetBitsTable, stateOffsetBits);
        FseCompressor.flush(blockStream, literalLengthTable, stateLiteralLength);

        int streamSize = blockStream.close();
        verify(streamSize > 0, "Output buffer too small");

        return streamSize;
    }

    private static int compressLiterals(
            CompressedBlockState.HuffmanTable previousHuffman,
            CompressedBlockState.HuffmanTable nextHuffman,
            CompressionParameters.Strategy strategy,
            boolean disableLiteralCompression,
            Object outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize)
    {
        // TODO
        return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
    }

    private static int rawLiterals(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize)
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

        DebugLog.print("Writing raw literals at offset %d, size: %d", outputAddress, headerSize + inputSize);

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
        DebugLog.print("Computing histogram. Input size = %d", length);
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

        for (int i = 0; i < maxSymbol; i++) {
            DebugLog.print("symbol = %d, count = %d", i, counts[i]);
        }
        
        return new Histogram(maxSymbol, largestCount, counts);
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
                DebugLog.print("Selected set_basic");
                return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
            }
            DebugLog.print("Selected set_rle");
            return new EncodingType(SEQUENCE_ENCODING_RLE, REPEAT_NONE);
        }

        if (strategy.ordinal() < CompressionParameters.Strategy.LAZY.ordinal()) { // TODO: more robust check. Maybe encapsulate in strategy objects
            if (isDefaultAllowed) {
                int staticFse_nbSeq_max = 1000;
                int mult = 10 - strategy.ordinal(); // TODO more robust
                int baseLog = 3;
                long dynamicFse_nbSeq_min = ((1L << defaultNormLog) * mult) >> baseLog;  /* 28-36 for offset, 56-72 for lengths */

                if ((repeatMode == REPEAT_VALID) && (sequenceCount < staticFse_nbSeq_max)) {
                    DebugLog.print("Selected set_repeat");
                    return new EncodingType(SEQUENCE_ENCODING_REPEAT, REPEAT_VALID);
                }

                if ((sequenceCount < dynamicFse_nbSeq_min) || (largestCount < (sequenceCount >> (defaultNormLog - 1)))) {
                    /* The format allows default tables to be repeated, but it isn't useful.
                     * When using simple heuristics to select encoding type, we don't want
                     * to confuse these tables with dictionaries. When running more careful
                     * analysis, we don't need to waste time checking both repeating tables
                     * and default tables.
                     */
                    DebugLog.print("Selected set_basic");
                    return new EncodingType(SEQUENCE_ENCODING_BASIC, REPEAT_NONE);
                }
            }
        }
        else {
            // TODO
            throw new UnsupportedOperationException("not yet implemented");
        }

        DebugLog.print("Selected set_compressed");
        return new EncodingType(SEQUENCE_ENCODING_COMPRESSED, CompressedBlockState.RepeatMode.REPEAT_CHECK);
    }

    private static int buildCompressionTable(Object outputBase, long outputAddress, int outputSize, int encodingType, byte[] codeTable, int[] counts, FseCompressionTable previous, int fseLog, int numberOfSequences, byte maxSymbol, short[] defaultNorm, int defaultNormLog, int defaultMax, FseCompressionTable nextCompressionTable)
    {
        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        DebugLog.print("Writing encoding type header at offset %d: %d", output, encodingType);

        switch (encodingType) {
            case SEQUENCE_ENCODING_RLE:
                UNSAFE.putByte(outputBase, output, codeTable[0]);
                FseCompressionTable.makeRleTable(nextCompressionTable, maxSymbol);
                return 1;
            case SEQUENCE_ENCODING_REPEAT:
                // TODO: copy previous -> next
                return 0;
            case SEQUENCE_ENCODING_BASIC:
                buildCompressionTable(nextCompressionTable, defaultNorm, defaultMax, defaultNormLog);
                return 0;
            case SEQUENCE_ENCODING_COMPRESSED:
                short[] norm = new short[MAX_SEQUENCES];
                int nbSeq_1 = numberOfSequences;

                int tableLog = optimalTableLog(fseLog, numberOfSequences, maxSymbol);
                if (counts[codeTable[numberOfSequences - 1]] > 1) {
                    counts[codeTable[numberOfSequences - 1]]--;
                    nbSeq_1--;
                }

                normalizeCounts(norm, tableLog, counts, nbSeq_1, maxSymbol);

                int size = writeNormalizedCounts(outputBase, output, (int) (outputLimit - output), norm, maxSymbol, tableLog); // TODO: pass outputLimit directly
                buildCompressionTable(nextCompressionTable, norm, maxSymbol, tableLog);

                DebugLog.print("Writing FSE table definition at offset %d, size = %d", output, size);
                return size;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    public static int normalizeCounts(short[] normalizedCounts, int tableLog, int[] counts, int total, int maxSymbol)
    {
        if (tableLog == 0) {
            tableLog = FiniteStateEntropy.DEFAULT_TABLE_LOG;
        }

        verify(tableLog >= FiniteStateEntropy.MIN_TABLE_LOG, "Unsupported FSE table size");
        verify(tableLog <= FiniteStateEntropy.MAX_TABLE_LOG, "FSE table size too large");
        verify(tableLog >= minTableLog(total, maxSymbol), "FSE table size too small");

        long scale = 62 - tableLog;
        long step = (1L << 62) / total;
        long vstep = 1L << (scale - 20);

        int stillToDistribute = 1 << tableLog;

        int largest = 0;
        short largestProbability = 0;
        int lowThreshold = total >>> tableLog;

        for (int symbol = 0; symbol <= maxSymbol; symbol++) {
            if (counts[symbol] == total) {
                return 0; // rle special case
            }
            if (counts[symbol] == 0) {
                normalizedCounts[symbol] = 0;
                continue;
            }
            if (counts[symbol] <= lowThreshold) {
                normalizedCounts[symbol] = -1;
                stillToDistribute--;
            }
            else {
                short probability = (short) ((counts[symbol] * step) >>> scale);
                DebugLog.print("symbol = %d, count = %d, probability = %d", symbol, counts[symbol], probability);
                if (probability < 8) {
                    long restToBeat = vstep * REST_TO_BEAT[probability];
                    DebugLog.print("rest-to-beat = %d, count[s]*step = %d, proba<<scale = %d", restToBeat, counts[symbol] * step, probability << scale);
                    long delta = counts[symbol] * step - (((long) probability) << scale);
                    if (delta > restToBeat) {
                        probability++;
                    }
                    DebugLog.print("probability = %d", probability);
                }
                if (probability > largestProbability) {
                    largestProbability = probability;
                    largest = symbol;
                }
                normalizedCounts[symbol] = probability;
                stillToDistribute -= probability;
            }
        }

        if (-stillToDistribute >= (normalizedCounts[largest] >>> 1)) {
            // corner case. Need another normalization method
            // TODO size_t const errorCode = FSE_normalizeM2(normalizedCounter, tableLog, count, total, maxSymbolValue);
            return normalizeCounts2(normalizedCounts, tableLog, counts, total, maxSymbol);
        }
        else {
            normalizedCounts[largest] += (short) stillToDistribute;
        }

        for (int i = 0; i <= maxSymbol; i++) {
            DebugLog.print("%3d: %4d", i, normalizedCounts[i]);
        }
        
        return tableLog;
//
//#if 0
//            {   /* Print Table (debug) */
//                U32 s;
//                U32 nTotal = 0;
//                for (s=0; s<=maxSymbolValue; s++)
//                    RAWLOG(2, "%3i: %4i \n", s, normalizedCounter[s]);
//                for (s=0; s<=maxSymbolValue; s++)
//                    nTotal += abs(normalizedCounter[s]);
//                if (nTotal != (1U<<tableLog))
//                RAWLOG(2, "Warning !!! Total == %u != %u !!!", nTotal, 1U<<tableLog);
//                getchar();
//            }
//#endif
//
//            return tableLog;
//        }
    }

    public int[] toInt(short[] elements)
    {
        int[] result = new int[elements.length];
        for (int i = 0; i < elements.length; i++) {
            result[i] = elements[i];
        }
        return result;
    }

    private static int normalizeCounts2(short[] normalizedCounts, int tableLog, int[] counts, int total, int maxSymbol)
    {
        int distributed = 0;

        int lowThreshold = total >>> tableLog; // minimum count below which frequency in the normalized table is "too small" (~ < 1)
        int lowOne = (total * 3) >>> (tableLog + 1); // 1.5 * lowThreshold. If count in (lowThreshold, lowOne] => assign frequency 1

        for (int i = 0; i < maxSymbol; i++) {
            if (counts[i] == 0) {
                normalizedCounts[i] = 0;
            }
            else if (counts[i] <= lowThreshold) {
                normalizedCounts[i] = -1;
                distributed++;
                total -= counts[i];
            }
            else if (counts[i] <= lowOne) {
                normalizedCounts[i] = 1;
                distributed++;
                total -= counts[i];
            }
            else {
                normalizedCounts[i] = UNASSIGNED;
            }
        }

        int normalizationFactor = 1 << tableLog;
        int toDistribute = normalizationFactor - distributed;

        if ((total / toDistribute) > lowOne) {
            /* risk of rounding to zero */
            lowOne = ((total * 3) / (toDistribute * 2));
            for (int i = 0; i <= maxSymbol; i++) {
                if ((normalizedCounts[i] == UNASSIGNED) && (counts[i] <= lowOne)) {
                    normalizedCounts[i] = 1;
                    distributed++;
                    total -= counts[i];
                }
            }
            toDistribute = normalizationFactor - distributed;
        }

        if (distributed == maxSymbol + 1) {
            // all values are pretty poor;
            // probably incompressible data (should have already been detected);
            // find max, then give all remaining points to max
            int maxValue = 0;
            int maxCount = 0;
            for (int i = 0; i <= maxSymbol; i++) {
                if (counts[i] > maxCount) {
                    maxValue = i;
                    maxCount = counts[i];
                }
            }
            normalizedCounts[maxValue] += (short) toDistribute;
            return 0;
        }

        if (total == 0) {
            // all of the symbols were low enough for the lowOne or lowThreshold
            for (int i = 0; toDistribute > 0; i = (i + 1) % (maxSymbol + 1)) {
                if (normalizedCounts[i] > 0) {
                    toDistribute--;
                    normalizedCounts[i]++;
                }
            }
            return 0;
        }

        // TODO: simplify/document this code
        long vStepLog = 62 - tableLog;
        long mid = (1L << (vStepLog - 1)) - 1;
        long rStep = (((1L << vStepLog) * toDistribute) + mid) / total;   /* scale on remaining */
        long tmpTotal = mid;
        for (int i = 0; i <= maxSymbol; i++) {
            if (normalizedCounts[i] == UNASSIGNED) {
                long end = tmpTotal + (counts[i] * rStep);
                int sStart = (int) (tmpTotal >>> vStepLog);
                int sEnd = (int) (end >>> vStepLog);
                int weight = sEnd - sStart;

                verify(weight >= 1, "Error"); // TODO
                normalizedCounts[i] = (short) weight;
                tmpTotal = end;
            }
        }

        return 0;
    }

    //    size_t FSE_buildCTable_wksp(FSE_CTable* ct, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog, void* workSpace, size_t wkspSize)
//    public static FseCompressionTable buildFseTable(short[] normalizedCounts, int maxSymbolValue, int tableLog)

    private static void buildCompressionTable(FseCompressionTable table, short[] normalizedCounts, int maxSymbolValue, int tableLog)
    {
//        assert(tableLog < 16);   /* required for the threshold strategy to work */
        int tableSize = 1 << tableLog;

//        verify(1 << tableLog <= tableSymbol.length, 0, "Table log too large");
        byte[] tableSymbol = new byte[tableSize]; // TODO: allocate per compressor (see wkspSize)
        int highThreshold = tableSize - 1;

        // TODO: make sure FseCompressionTable has enough size
        table.log2Size = tableLog;
        table.maxSymbol = maxSymbolValue;

        /* For explanations on how to distribute symbol values over the table :
         *  http://fastcompression.blogspot.fr/2014/02/fse-distributing-symbol-values.html */

        // symbol start positions
        int[] cumulative = new int[FSE_MAX_SYMBOL_VALUE + 2];
        cumulative[0] = 0;
        for (int i = 1; i <= maxSymbolValue + 1; i++) {
            if (normalizedCounts[i - 1] == -1) {  /* Low probability symbol */
                cumulative[i] = cumulative[i - 1] + 1;
                tableSymbol[highThreshold--] = (byte) (i - 1);
            }
            else {
                cumulative[i] = cumulative[i - 1] + normalizedCounts[i - 1];
            }
        }
        cumulative[maxSymbolValue + 1] = tableSize + 1;

        // Spread symbols
        int position = FiniteStateEntropy.spreadSymbols(normalizedCounts, maxSymbolValue, tableSize, highThreshold, tableSymbol);

        verify(position == 0, "Spread symbols failed");

        // Build table
        for (int i = 0; i < tableSize; i++) {
            byte symbol = tableSymbol[i];
            table.nextState[cumulative[symbol]++] = (short) (tableSize + i);  /* TableU16 : sorted by symbol order; gives next state value */
        }

        // Build Symbol Transformation Table
        int total = 0;
        for (int symbol = 0; symbol <= maxSymbolValue; symbol++) {
            switch (normalizedCounts[symbol]) {
                case 0:
                    /* filling nonetheless, for compatibility with FSE_getMaxNbBits() */
                    table.deltaNumberOfBits[symbol] = ((tableLog + 1) << 16) - tableSize;
                    break;
                case -1:
                case 1:
                    table.deltaNumberOfBits[symbol] = (tableLog << 16) - tableSize;
                    table.deltaFindState[symbol] = total - 1;
                    total++;
                    break;
                default:
                    int maxBitsOut = tableLog - Util.highestBit(normalizedCounts[symbol] - 1);
                    int minStatePlus = normalizedCounts[symbol] << maxBitsOut;
                    table.deltaNumberOfBits[symbol] = (maxBitsOut << 16) - minStatePlus;
                    table.deltaFindState[symbol] = total - normalizedCounts[symbol];
                    total += normalizedCounts[symbol];
                    break;
            }
        }
    }

//    public static void main1(String[] args)
//    {
//        FseCompressionTable table = new FseCompressionTable(LITERALS_LENGTH_DEFAULT_NORM_LOG, MAX_LITERALS_LENGTH_SYMBOL);
//        buildCompressionTable(table, LITERALS_LENGTH_DEFAULT_NORMS, MAX_LITERALS_LENGTH_SYMBOL, LITERALS_LENGTH_DEFAULT_NORM_LOG);
//
//        byte[] original = new byte[100];
//        for (int i = 0; i < original.length; i++) {
//            original[i] = (byte) (i % 10);
//        }
//
//        byte[] compressed = new byte[100];
//        byte[] decompressed = new byte[100];
//
//        int compressedSize = FseCompressor.compress(compressed, 16, compressed.length, original, 16, original.length, table);
//        int decompressedSize = FiniteStateEntropy.decompress(DEFAULT_LITERALS_LENGTH_TABLE, compressed, 16, 16 + compressedSize, decompressed);
//
//        System.out.println(compressedSize);
//        System.out.println(decompressedSize);
//
//        for (int i = 0; i < decompressedSize; i++) {
//            if (original[i] != decompressed[i]) {
//                throw new RuntimeException();
//            }
//        }
//    }

//    public static void main(String[] args)
//    {
//        SequenceCompressor compressor = new SequenceCompressor();
//
//        Random random = new Random();
//
//        int maxSymbol = 255;
//
//        int total = 0;
//        int[] counts = new int[maxSymbol + 1];
//        for (int i = 0; i < counts.length; i++) {
////            counts[i] = Math.max(0, (int) (random.nextGaussian() * 1000));
//            counts[i] = i;
//            total += counts[i];
//        }
//
//        short[] normalizedCounts = new short[counts.length];
//
//        int tableLog = FiniteStateEntropy.MAX_TABLE_LOG;
//        compressor.normalizeCounts(normalizedCounts, tableLog, counts, total, maxSymbol);
//
//        int sum = 0;
//        for (int i = 0; i <= maxSymbol; i++) {
//            sum += Math.abs(normalizedCounts[i]);
//        }
//        System.out.println("total normalized: " + sum);
//        if (sum != 1 << tableLog) {
//            throw new IllegalStateException();
//        }
//
//        byte[] buffer = new byte[1000];
//        int size = compressor.writeNormalizedCounts(buffer, 16, buffer.length, normalizedCounts, maxSymbol, tableLog);
//
//        System.out.println(size);
//    }
}
