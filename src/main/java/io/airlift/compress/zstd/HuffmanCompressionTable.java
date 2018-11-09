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

import static io.airlift.compress.zstd.Huffman.MAX_FSE_TABLE_LOG;
import static io.airlift.compress.zstd.Huffman.MAX_SYMBOL;
import static io.airlift.compress.zstd.Huffman.MAX_TABLE_LOG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;

public final class HuffmanCompressionTable
{
    final short[] values;
    final byte[] numberOfBits;
    int maxSymbol;

    int maxNumberOfBits;

    public HuffmanCompressionTable(int capacity)
    {
        this.values = new short[capacity];
        this.numberOfBits = new byte[capacity];
    }

    public int write(Object outputBase, long outputAddress, int outputSize, HuffmanTableWriterWorkspace workspace)
    {
        verify(maxSymbol <= MAX_SYMBOL, "Max symbol too large");

        byte[] weights = workspace.weights;

        long output = outputAddress;

        int maxNumberOfBits = this.maxNumberOfBits;
        int maxSymbol = this.maxSymbol;

        // convert to weights per RFC 8478 section 4.2.1
        for (int symbol = 0; symbol < maxSymbol; symbol++) {
            int bits = numberOfBits[symbol];

            if (bits == 0) {
                weights[symbol] = 0;
            }
            else {
                weights[symbol] = (byte) (maxNumberOfBits + 1 - bits);
            }
        }

        // attempt weights compression by FSE
        int size = compressWeights(outputBase, output + 1, outputSize - 1, weights, maxSymbol, workspace);

        if (maxSymbol > 127 && size > 127) {
            // This should never happen. Since weights are in the range [0, 12], they can be compressed optimally to ~3.7 bits per symbol for a uniform distribution.
            // Since maxSymbol has to be <= MAX_SYMBOL (255), this is 119 bytes + FSE headers.
            throw new AssertionError();
        }

        if (size != 0 && size != 1 && size < maxSymbol / 2) {
            // Go with FSE only if:
            //   - the weights are compressible
            //   - the compressed size is better than what we'd get with the raw encoding below
            //   - the compressed size is <= 127 bytes, which is the most that the encoding can hold for FSE-compressed weights (see RFC 8478 section 4.2.1.1). This is implied
            //     by the maxSymbol / 2 check, since maxSymbol must be <= 255
            UNSAFE.putByte(outputBase, output, (byte) size);
            return size + 1; // header + size
        }
        else {
            // Use raw encoding (4 bits per entry)

            // #entries = #symbols - 1 since last symbol is implicit. Thus, #entries = (maxSymbol + 1) - 1 = maxSymbol
            int entryCount = maxSymbol;

            size = (entryCount + 1) / 2;  // ceil(#entries / 2)
            verify(size + 1 /* header */ <= outputSize, "Output size too small"); // 2 entries per byte

            // encode number of symbols
            // header = #entries + 127 per RFC
            UNSAFE.putByte(outputBase, output, (byte) (127 + entryCount));
            output++;

            weights[maxSymbol] = 0; // last weight is implicit, so set to 0 so that it doesn't get encoded below
            for (int i = 0; i < entryCount; i += 2) {
                UNSAFE.putByte(outputBase, output, (byte) ((weights[i] << 4) + weights[i + 1]));
                output++;
            }

            return (int) (output - outputAddress);
        }
    }

    /**
     * Can this table encode all symbols with non-zero count?
     */
    public boolean isValid(int[] counts, int maxSymbol)
    {
        if (maxSymbol > this.maxSymbol) {
            // some non-zero count symbols cannot be encoded by the current table
            return false;
        }

        for (int symbol = 0; symbol <= maxSymbol; ++symbol) {
            if (counts[symbol] != 0 && numberOfBits[symbol] == 0) {
                return false;
            }
        }
        return true;
    }

    public int estimateCompressedSize(int[] counts, int maxSymbol)
    {
        int numberOfBits = 0;
        for (int symbol = 0; symbol <= Math.min(maxSymbol, this.maxSymbol); symbol++) {
            numberOfBits += this.numberOfBits[symbol] * counts[symbol];
            DebugLog.print("symbol %d: bits=%d, count=%d, total-so-far=%d", symbol, this.numberOfBits[symbol], counts[symbol], numberOfBits);
        }

        return numberOfBits >>> 3; // convert to bytes
    }

    /**
     * All elements within weightTable must be <= Huffman.MAX_TABLE_LOG
     */
    private static int compressWeights(Object outputBase, long outputAddress, int outputSize, byte[] weights, int weightsLength, HuffmanTableWriterWorkspace workspace)
    {
        if (weightsLength <= 1) {
            return 0; // Not compressible
        }

        // Scan input and build symbol stats
        Histogram histogram = Histogram.count(weights, weightsLength, MAX_TABLE_LOG, workspace.counts);

        int maxCount = histogram.largestCount;
        int maxSymbol = histogram.maxSymbol;
        int[] counts = histogram.counts;

        if (maxCount == weightsLength) {
            return 1; // only a single symbol in source
        }
        if (maxCount == 1) {
            return 0; // each symbol present maximum once => not compressible
        }

        short[] normalizedCounts = workspace.normalizedCounts;

        int tableLog = FiniteStateEntropy.optimalTableLog(MAX_FSE_TABLE_LOG, weightsLength, maxSymbol);
        FiniteStateEntropy.normalizeCounts(normalizedCounts, tableLog, counts, weightsLength, maxSymbol);

        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        // Write table description header
        int headerSize = FiniteStateEntropy.writeNormalizedCounts(outputBase, output, outputSize, normalizedCounts, maxSymbol, tableLog);
        output += headerSize;

        // Compress
        FseCompressionTable compressionTable = workspace.fseTable;
        FiniteStateEntropy.buildCompressionTable(compressionTable, normalizedCounts, maxSymbol, tableLog);
        int compressedSize = FseCompressor.compress(outputBase, output, (int) (outputLimit - output), weights, weightsLength, compressionTable);
        if (compressedSize == 0) {
            return 0; // not enough space for compressed data
        }
        output += compressedSize;

        return (int) (output - outputAddress);
    }
}
