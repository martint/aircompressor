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

import java.util.Arrays;

import static io.airlift.compress.zstd.FiniteStateEntropy.minTableLog;
import static io.airlift.compress.zstd.Huffman.MAX_FSE_TABLE_LOG;
import static io.airlift.compress.zstd.Huffman.MAX_SYMBOL;
import static io.airlift.compress.zstd.Huffman.MAX_SYMBOL_COUNT;
import static io.airlift.compress.zstd.Huffman.MAX_TABLE_LOG;
import static io.airlift.compress.zstd.Huffman.MIN_TABLE_LOG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;

public final class HuffmanCompressionTable
{
    private static final int STARTNODE = MAX_SYMBOL_COUNT;

    private final short[] values;
    private final byte[] numberOfBits;

    private int maxSymbol;
    private int maxNumberOfBits;

    public HuffmanCompressionTable(int capacity)
    {
        this.values = new short[capacity];
        this.numberOfBits = new byte[capacity];
    }

    public static int optimalNumberOfBits(int maxNumberOfBits, int inputSize, int maxSymbol)
    {
        if (inputSize <= 1) {
            throw new IllegalArgumentException(); // not supported. Use RLE instead
        }

        int result = maxNumberOfBits;

        result = Math.min(result, Util.highestBit((inputSize - 1)) - 1); // we may be able to reduce accuracy if input is small

        // Need a minimum to safely represent all symbol values
        result = Math.max(result, minTableLog(inputSize, maxSymbol));

        result = Math.max(result, MIN_TABLE_LOG); // absolute minimum for Huffman
        result = Math.min(result, MAX_TABLE_LOG); // absolute maximum for Huffman

        return result;
    }

    public void initialize(int[] counts, int maxSymbol, int maxNumberOfBits, HuffmanCompressionTableWorkspace workspace)
    {
        workspace.reset();

        NodeTable nodeTable = workspace.nodeTable;
        short[] numberOfBitsPerRank = workspace.numberOfBitsPerRank;
        short[] valuesPerRank = workspace.valuesPerRank;

        // TODO: native code passes nodeTable[1] as the base pointer for operating here. We simulate that by passing an "offset". Figure out why it's needed and
        // come up with a better way
        int offset = 1; // TODO to simulate "huffNode" in the native code

        verify(maxSymbol <= MAX_SYMBOL, "Max symbol value too large");

        nodeTable.reset();

        // sort, decreasing order
        sort(nodeTable, offset, counts, maxSymbol, workspace);

        // init for parents
        short nodeNb = STARTNODE;
        int nonNullRank = maxSymbol;
        while (nodeTable.count[nonNullRank + offset] == 0) {
            nonNullRank--;
        }
        int lowS = nonNullRank;
        int nodeRoot = nodeNb + lowS - 1;
        int lowN = nodeNb;

        nodeTable.count[nodeNb + offset] = nodeTable.count[lowS + offset] + nodeTable.count[lowS - 1 + offset];
        nodeTable.parents[lowS + offset] = nodeNb;
        nodeTable.parents[lowS - 1 + offset] = nodeNb;
        nodeNb++;
        lowS -= 2;

        for (int n = nodeNb; n <= nodeRoot; n++) {
            nodeTable.count[n + offset] = 1 << 30;
        }
        nodeTable.count[0] = (int) ((1L << 31) - 1); // fake entry, strong barrier

        // create parents
        while (nodeNb <= nodeRoot) {
            int child1 = (nodeTable.count[lowS + offset] < nodeTable.count[lowN + offset]) ? lowS-- : lowN++;
            int child2 = (nodeTable.count[lowS + offset] < nodeTable.count[lowN + offset]) ? lowS-- : lowN++;
            nodeTable.count[nodeNb + offset] = nodeTable.count[child1 + offset] + nodeTable.count[child2 + offset];
            nodeTable.parents[child1 + offset] = nodeNb;
            nodeTable.parents[child2 + offset] = nodeNb;
            nodeNb++;
        }

        // distribute weights (unlimited tree height)
        nodeTable.numberOfBits[nodeRoot + offset] = 0;
        for (int n = nodeRoot - 1; n >= STARTNODE; n--) {
            short parent = nodeTable.parents[n + offset];
            nodeTable.numberOfBits[n + offset] = (byte) (nodeTable.numberOfBits[parent + offset] + 1);
        }

        for (int n = 0; n <= nonNullRank; n++) {
            nodeTable.numberOfBits[n + offset] = (byte) (nodeTable.numberOfBits[nodeTable.parents[n + offset] + offset] + 1);
        }

        DebugLog.print("Huffman node table");
        for (int i = 0; i < nodeTable.count.length; i++) {
            DebugLog.print("%3d: count: %5d, symbol: %3d, bits: %2d, parent: %3d", i, nodeTable.count[i], nodeTable.bytes[i] & 0xFF, nodeTable.numberOfBits[i], nodeTable.parents[i]);
        }

        // enforce max table log
        maxNumberOfBits = setMaxHeight(nodeTable, offset, nonNullRank, maxNumberOfBits, workspace);

        DebugLog.print("Huffman node table -- after max height enforcement");
        for (int i = 0; i < nodeTable.count.length; i++) {
            DebugLog.print("%3d: count: %5d, symbol: %3d, bits: %2d, parent: %3d", i, nodeTable.count[i], nodeTable.bytes[i] & 0xFF, nodeTable.numberOfBits[i], nodeTable.parents[i]);
        }

        // fill result into tree (val, nbBits)
        verify(maxNumberOfBits <= MAX_TABLE_LOG, "Max number of bits larger than max table size"); // TODO: error message  // check fit into table
        for (int n = 0; n <= nonNullRank; n++) {
            numberOfBitsPerRank[nodeTable.numberOfBits[n + offset]]++;
        }
        // determine stating value per rank
        short min = 0;
        for (int n = maxNumberOfBits; n > 0; n--) {
            valuesPerRank[n] = min; // get starting value within each rank
            min += numberOfBitsPerRank[n];
            min >>>= 1;
        }

        for (int n = 0; n <= maxSymbol; n++) {
            numberOfBits[nodeTable.bytes[n + offset] & 0xFF] = nodeTable.numberOfBits[n + offset];  // push nbBits per symbol, symbol order
        }
        for (int n = 0; n <= maxSymbol; n++) {
            values[n] = valuesPerRank[numberOfBits[n]]++; // assign value within rank, symbol order
        }

        this.maxSymbol = maxSymbol;
        this.maxNumberOfBits = maxNumberOfBits;

        DebugLog.print("Huffman compression table");
        for (int i = 0; i < maxSymbol; i++) {
            DebugLog.print("symbol: %3d => value: %5d, bits: %d", i, values[i], numberOfBits[i]);
        }
    }

    // TODO: consider encoding 2 symbols at a time
    //   - need a table with 256x256 entries with
    //      - the concatenated bits for the corresponding pair of symbols
    //      - the sum of bits for the corresponding pair of symbols
    //   - read 2 symbols at a time from the input
    public void encodeSymbol(BitstreamEncoder output, int symbol)
    {
//        DebugLog.print("Encoded symbol: %3d => %6d (bits: %2d)", symbol, table.values[symbol], table.numberOfBits[symbol]);
        output.addBitsFast(values[symbol], numberOfBits[symbol]);
    }


    // http://fastcompression.blogspot.com/2015/07/huffman-revisited-part-3-depth-limited.html

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

    private static void sort(NodeTable nodeTable, int offset, int[] counts, int maxSymbolValue, HuffmanCompressionTableWorkspace workspace)
    {
        int[] rankBase = workspace.rankBase;
        int[] rankCurrent = workspace.rankCurrent;

        Arrays.fill(rankBase, 0);

        for (int n = 0; n <= maxSymbolValue; ++n) {
            int rank = Util.highestBit(counts[n] + 1);
            rankBase[rank]++;
        }

        for (int n = 30; n > 0; n--) {
            rankBase[n - 1] += rankBase[n];
        }

        for (int n = 0; n < 32; n++) {
            rankCurrent[n] = rankBase[n];
        }

        for (int n = 0; n <= maxSymbolValue; n++) {
            int count = counts[n];
            int rank = Util.highestBit(count + 1) + 1;
            int pos = rankCurrent[rank]++;

            while (pos > rankBase[rank] && count > nodeTable.count[pos - 1 + offset]) {
                nodeTable.copyNode(pos - 1 + offset, pos + offset);
                pos--;
            }

            nodeTable.count[pos + offset] = count;
            nodeTable.bytes[pos + offset] = (byte) n;
        }
    }

    private static int setMaxHeight(NodeTable nodeTable, int offset, int lastNonNull, int maxNbBits, HuffmanCompressionTableWorkspace workspace)
    {
        int largestBits = nodeTable.numberOfBits[lastNonNull + offset];

        if (largestBits <= maxNbBits) {
            return largestBits;   // early exit : no elements > maxNbBits
        }

        // there are several too large elements (at least >= 2)
        int totalCost = 0;
        int baseCost = 1 << (largestBits - maxNbBits);
        int n = lastNonNull;

        while (nodeTable.numberOfBits[n + offset] > maxNbBits) {
            totalCost += baseCost - (1 << (largestBits - nodeTable.numberOfBits[n + offset]));
            nodeTable.numberOfBits[n + offset] = (byte) maxNbBits;
            n--;
        }  // n stops at nodeTable.numberOfBits[n + offset].nbBits <= maxNbBits

        while (nodeTable.numberOfBits[n + offset] == maxNbBits) {
            n--;   // n ends at index of smallest symbol using < maxNbBits
        }

        // renorm totalCost
        totalCost >>>= (largestBits - maxNbBits);  // note : totalCost is necessarily a multiple of baseCost

        // repay normalized cost
        int noSymbol = 0xF0F0F0F0;
        int[] rankLast = workspace.rankLast;
        Arrays.fill(rankLast, noSymbol);

        // Get pos of last (smallest) symbol per rank
        int currentNbBits = maxNbBits;
        for (int pos = n; pos >= 0; pos--) {
            if (nodeTable.numberOfBits[pos + offset] >= currentNbBits) {
                continue;
            }
            currentNbBits = nodeTable.numberOfBits[pos + offset];   // < maxNbBits
            rankLast[maxNbBits - currentNbBits] = pos;
        }

        while (totalCost > 0) {
            int nBitsToDecrease = Util.highestBit(totalCost) + 1;
            for (; nBitsToDecrease > 1; nBitsToDecrease--) {
                int highPos = rankLast[nBitsToDecrease];
                int lowPos = rankLast[nBitsToDecrease - 1];
                if (highPos == noSymbol) {
                    continue;
                }
                if (lowPos == noSymbol) {
                    break;
                }
                int highTotal = nodeTable.count[highPos + offset];
                int lowTotal = 2 * nodeTable.count[lowPos + offset];
                if (highTotal <= lowTotal) {
                    break;
                }
            }

            // only triggered when no more rank 1 symbol left => find closest one (note : there is necessarily at least one !)
            // HUF_MAX_TABLELOG test just to please gcc 5+; but it should not be necessary
            while ((nBitsToDecrease <= MAX_TABLE_LOG) && (rankLast[nBitsToDecrease] == noSymbol)) {
                nBitsToDecrease++;
            }
            totalCost -= 1 << (nBitsToDecrease - 1);
            if (rankLast[nBitsToDecrease - 1] == noSymbol) {
                rankLast[nBitsToDecrease - 1] = rankLast[nBitsToDecrease];   // this rank is no longer empty
            }
            nodeTable.numberOfBits[rankLast[nBitsToDecrease] + offset]++;
            if (rankLast[nBitsToDecrease] == 0) {   /* special case, reached largest symbol */
                rankLast[nBitsToDecrease] = noSymbol;
            }
            else {
                rankLast[nBitsToDecrease]--;
                if (nodeTable.numberOfBits[rankLast[nBitsToDecrease] + offset] != maxNbBits - nBitsToDecrease) {
                    rankLast[nBitsToDecrease] = noSymbol;   // this rank is now empty
                }
            }
        }

        while (totalCost < 0) {  // Sometimes, cost correction overshoot
            if (rankLast[1] == noSymbol) {  /* special case : no rank 1 symbol (using maxNbBits-1); let's create one from largest rank 0 (using maxNbBits) */
                while (nodeTable.numberOfBits[n + offset] == maxNbBits) {
                    n--;
                }
                nodeTable.numberOfBits[n + 1 + offset]--;
                rankLast[1] = n + 1;
                totalCost++;
                continue;
            }
            nodeTable.numberOfBits[rankLast[1] + 1 + offset]--;
            rankLast[1]++;
            totalCost++;
        }

        return maxNbBits;
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
