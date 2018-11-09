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

import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.FiniteStateEntropy.minTableLog;
import static io.airlift.compress.zstd.Huffman.MAX_SYMBOL;
import static io.airlift.compress.zstd.Huffman.MAX_SYMBOL_COUNT;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;

public class HuffmanCompressor
{
    public static int optimalTableLog(int maxTableLog, int srcSize, int maxSymbolValue)
    {
        // TODO: same as FSE.optimalTableLog but with "- 1" instead of "- 2" in maxBitsSrc
        if (srcSize <= 1) {
            throw new IllegalArgumentException(); // not supported. Use RLE instead
        }

        int tableLog = maxTableLog;

        int maxBitsSrc = Util.highestBit((srcSize - 1)) - 1;
        if (maxBitsSrc < tableLog) {
            tableLog = maxBitsSrc;   /* Accuracy can be reduced */
        }

        int minBits = minTableLog(srcSize, maxSymbolValue);
        if (minBits > tableLog) {
            tableLog = minBits;   /* Need a minimum to safely represent all symbol values */
        }

        if (tableLog < Huffman.MIN_TABLE_LOG) {
            tableLog = Huffman.MIN_TABLE_LOG;
        }

        if (tableLog > Huffman.MAX_TABLE_LOG) {
            tableLog = Huffman.MAX_TABLE_LOG;
        }

        return tableLog;
    }

    private static final int STARTNODE = MAX_SYMBOL_COUNT;

    public static void buildCompressionTable(HuffmanCompressionTable table, int[] counts, int maxSymbol, int maxNumberOfBits, HuffmanCompressionTableWorkspace workspace)
    {
        workspace.reset();
        
        NodeTable nodeTable = workspace.nodeTable;
        short[] numberOfBitsPerRank = workspace.numberOfBitsPerRank;
        short[] valuesPerRank = workspace.valuesPerRank; 

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
        verify(maxNumberOfBits <= Huffman.MAX_TABLE_LOG, "Max number of bits larger than max table size"); // TODO: error message  // check fit into table
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
            table.numberOfBits[nodeTable.bytes[n + offset] & 0xFF] = nodeTable.numberOfBits[n + offset];  // push nbBits per symbol, symbol order
        }
        for (int n = 0; n <= maxSymbol; n++) {
            table.values[n] = valuesPerRank[table.numberOfBits[n]]++; // assign value within rank, symbol order
        }

        table.maxSymbol = maxSymbol;
        table.maxNumberOfBits = maxNumberOfBits;

        DebugLog.print("Huffman compression table");
        for (int i = 0; i < maxSymbol; i++) {
            DebugLog.print("symbol: %3d => value: %5d, bits: %d", i, table.values[i], table.numberOfBits[i]);
        }
    }

    // TODO: native code passes nodeTable[1] as the base pointer for operating here. We simulate that by passing an "offset". Figure out why it's needed and
    // come up with a better way
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

    // http://fastcompression.blogspot.com/2015/07/huffman-revisited-part-3-depth-limited.html
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
            n--;   // n end at index of smallest symbol using < maxNbBits
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
            while ((nBitsToDecrease <= Huffman.MAX_TABLE_LOG) && (rankLast[nBitsToDecrease] == noSymbol)) {
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

//    public static int compress(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize, boolean singleStream, HuffmanCompressionTable table, int headerSize)
//    {
//        int compressedSize;
//
//        if (singleStream) {
//            compressedSize = compressSingleStream(outputBase, outputAddress, outputSize, inputBase, inputAddress, inputSize, table);
//        }
//        else {
//            compressedSize = compress4streams(outputBase, outputAddress, outputSize, inputBase, inputAddress, inputSize, table);
//        }
//
//        int result;
//        if (compressedSize == 0) {
//            result = 0; // incompressible
//        }
//        else if (compressedSize + headerSize >= inputSize - 1) {
//            result = 0;
//        }
//        else {
//            result = compressedSize + headerSize;
//        }
//
//        return result;
//    }

    public static int compress4streams(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table)
    {
        long input = inputAddress;
        long inputLimit = inputAddress + inputSize;
        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        int segmentSize = (inputSize + 3) / 4;

        if (outputSize < 6 /* jump table */ + 1 /* first stream */ + 1 /* second stream */ + 1 /* third stream */ + 8 /* 8 bytes minimum needed by the bitstream encoder */) {
            return 0; // minimum space to compress successfully
        }

        if (inputSize <= 6 + 1 + 1 + 1) { // jump table + one byte per stream
            return 0;  // no saving possible: input too small
        }

        output += SIZE_OF_SHORT + SIZE_OF_SHORT + SIZE_OF_SHORT; // jump table

        int compressedSize;

        // first segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
//        DebugLog.print("first segment: %d", compressedSize);
        if (compressedSize == 0) {
            return 0;
        }
        UNSAFE.putShort(outputBase, outputAddress, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // second segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
//        DebugLog.print("second segment: %d", compressedSize);
        if (compressedSize == 0) {
            return 0;
        }
        UNSAFE.putShort(outputBase, outputAddress + SIZE_OF_SHORT, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // third segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
//        DebugLog.print("third segment: %d", compressedSize);
        if (compressedSize == 0) {
            return 0;
        }
        UNSAFE.putShort(outputBase, outputAddress + SIZE_OF_SHORT + SIZE_OF_SHORT, (short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // fourth segment
        compressedSize = compressSingleStream(outputBase, output, (int) (outputLimit - output), inputBase, input, (int) (inputLimit - input), table);
//        DebugLog.print("fourth segment: %d", compressedSize);
        if (compressedSize == 0) {
            return 0;
        }
        output += compressedSize;

        return (int) (output - outputAddress);
    }

    public static int compressSingleStream(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table)
    {
        BitstreamEncoder bitstream = new BitstreamEncoder(outputBase, outputAddress, outputSize);
        long input = inputAddress;

        int n = inputSize & ~3; // join to mod 4

        switch (inputSize & 3) {
            case 3:
                encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n + 2) & 0xFF, table);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 2:
                encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n + 1) & 0xFF, table);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 1:
                encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n + 0) & 0xFF, table);
                bitstream.flush();
                // fall-through
            case 0: /* fall-through */
            default:
                break;
        }

        for (; n > 0; n -= 4) {  // note : n & 3==0 at this stage
            encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n - 1) & 0xFF, table);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n - 2) & 0xFF, table);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                bitstream.flush();
            }
            encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n - 3) & 0xFF, table);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            encodeSymbol(bitstream, UNSAFE.getByte(inputBase, input + n - 4) & 0xFF, table);
            bitstream.flush();
        }

        return bitstream.close();
    }

    // TODO: consider encoding 2 symbols at a time
    //   - need a table with 256x256 entries with
    //      - the concatenated bits for the corresponding pair of symbols
    //      - the sum of bits for the corresponding pair of symbols
    //   - read 2 symbols at a time from the input
    private static void encodeSymbol(BitstreamEncoder bitstream, int symbol, HuffmanCompressionTable table)
    {
        verify(symbol <= table.maxSymbol, "symbol not in table");
//        DebugLog.print("Encoded symbol: %3d => %6d (bits: %2d)", symbol, table.values[symbol], table.numberOfBits[symbol]);
        bitstream.addBitsFast(table.values[symbol], table.numberOfBits[symbol]);
    }

    //    public static void main(String[] args)
//    {
////        HuffmanCompressionTable table = new HuffmanCompressionTable();
////        int[] counts = new int[256];
////        for (int i = 0; i < counts.length; i++) {
////            counts[i] = i;
////        }
////
////        HuffmanCompressionContext.NodeTable nodeTable = new HuffmanCompressionContext.NodeTable(HUF_CTABLE_WORKSPACE_SIZE_U32);
//////        HuffmanCompressor.sort(nodeTable, 1, counts, counts.length - 1);
////        HuffmanCompressor.buildCompressionTable(table, counts, counts.length - 1, HUF_TABLELOG_MAX, new HuffmanCompressionContext.NodeTable(HUF_CTABLE_WORKSPACE_SIZE_U32));
//
//        byte[] original = new byte[351];
//        int i = 0;
//        int repetitions = 1;
//        for (int symbol = 'a'; symbol <= 'z'; symbol++) {
//            for (int j = 0; j < repetitions; j++) {
//                original[i] = (byte) symbol;
//                i++;
//            }
//            repetitions++;
//        }
//
//        byte[] compressed = new byte[2000];
//
//        HuffmanCompressionContext context = new HuffmanCompressionContext();
//        int compressedSize = HuffmanCompressor.compress(compressed, 16, compressed.length, original, 16, original.length, 255, 11, true, context, null, false);
//        System.out.println(compressedSize);
//
//        byte[] decompressed = new byte[500];
//        Huffman huffman = new Huffman();
//
//        long input = 16;
//        int headerSize = huffman.readTable(compressed, input, compressedSize);
//        input += headerSize;
//        System.out.println(headerSize);
//        huffman.decodeSingleStream(compressed, input, 16 + compressedSize, decompressed, 16, 16 + original.length);
//
//        for (int x = 0; i < original.length; x++) {
//            if (original[x] != decompressed[x]) {
//                throw new RuntimeException();
//            }
//        }
//
//        System.out.println();
//    }
}
