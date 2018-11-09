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

import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class Histogram
{
    final int maxSymbol;
    final int largestCount;
    final int[] counts;

    private Histogram(int maxSymbol, int largestCount, int[] counts)
    {
        this.maxSymbol = maxSymbol;
        this.largestCount = largestCount;
        this.counts = counts;
    }

    private static Histogram count(Object inputBase, long inputAddress, int inputSize, int maxSymbol, int[] workspace)
    {
//        DebugLog.print("Computing histogram. Input size = %d", inputSize);
        long input = inputAddress;

        // TODO: HIST_count_parallel_wksp heuristic

        int[] counts = workspace;
        Arrays.fill(counts, 0);

        int largestCount = 0;
        if (inputSize != 0) {
            for (int i = 0; i < inputSize; i++) {
                int symbol = UNSAFE.getByte(inputBase, input) & 0xFF;
                input++;
                counts[symbol]++;
            }

            while (counts[maxSymbol] == 0) {
                maxSymbol--;
            }

            for (int i = 0; i <= maxSymbol; i++) {
                if (counts[i] > largestCount) {
                    largestCount = counts[i];
                }
            }
        }

        return new Histogram(maxSymbol, largestCount, counts); // TODO; counts is being passed in, so shouldn't have to return it
    }

    public static Histogram count(byte[] input, int length, int maxSymbol, int[] workspace)
    {
        return count(input, ARRAY_BYTE_BASE_OFFSET, length, maxSymbol, workspace);
    }
}
