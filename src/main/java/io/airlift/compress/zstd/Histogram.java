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

import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class Histogram
{
    // TODO: access modifiers
    int maxSymbol;
    int largestCount;
    final int[] counts;

    private Histogram(int maxSymbol, int largestCount, int[] counts)
    {
        this.maxSymbol = maxSymbol;
        this.largestCount = largestCount;
        this.counts = counts;
    }

    public static Histogram count(Object inputBase, long inputAddress, int inputSize, int maxSymbol)
    {
//        DebugLog.print("Computing histogram. Input size = %d", inputSize);

        long input = inputAddress;
        long inputLimit = inputAddress + inputSize;

        // TODO: HIST_count_parallel_wksp heuristic

        // TODO: allocate once per compressor & fill with 0s:
        //  int[] counts = new int[MAX_SEQUENCES + 1]; for sequence encoder
        //  int[] counts = new int[HUF_SYMBOLVALUE_MAX + 1]; for huffman encoder
        int[] counts = new int[maxSymbol + 1];

        if (inputSize == 0) {
            return new Histogram(0, 0, counts);
        }

        while (input < inputLimit) {
            int symbol = UNSAFE.getByte(inputBase, input) & 0xFF;
            input++;
            counts[symbol]++;
        }

        while (counts[maxSymbol] == 0) {
            maxSymbol--;
        }

        int largestCount = 0;
        for (int i = 0; i <= maxSymbol; i++) {
            if (counts[i] > largestCount) {
                largestCount = counts[i];
            }
        }

//        for (int i = 0; i <= maxSymbol; i++) {
//            DebugLog.print("symbol = %d, count = %d", i, counts[i]);
//        }

        return new Histogram(maxSymbol, largestCount, counts);
    }

    public static Histogram count(byte[] input, int length, int maxSymbol)
    {
        return count(input, ARRAY_BYTE_BASE_OFFSET, length, maxSymbol);
    }
}
