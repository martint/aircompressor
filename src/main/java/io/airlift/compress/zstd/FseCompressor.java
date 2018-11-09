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

import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.verify;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class FseCompressor
{
    private FseCompressor()
    {
    }

    public static int compress(Object outputBase, long outputAddress, int outputSize, byte[] input, int inputSize, FseCompressionTable table)
    {
        return compress(outputBase, outputAddress, outputSize, input, ARRAY_BYTE_BASE_OFFSET, inputSize, table);
    }

    public static int compress(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize, FseCompressionTable table)
    {
        verify(outputSize >= SIZE_OF_LONG, -1, "Output size too small"); // TODO

        long start = inputAddress;
        long inputLimit = start + inputSize;

        long input = inputLimit;

        if (inputSize <= 2) {
            return 0;
        }

        BitstreamEncoder stream = new BitstreamEncoder(outputBase, outputAddress, outputSize);

        int state1;
        int state2;

        if ((inputSize & 1) != 0) {
            input--;
            state1 = initialize(table, UNSAFE.getByte(inputBase, input));

            input--;
            state2 = initialize(table, UNSAFE.getByte(inputBase, input));

            input--;
            state1 = encode(stream, table, state1, UNSAFE.getByte(inputBase, input));

            stream.flush();
        }
        else {
            input--;
            state2 = initialize(table, UNSAFE.getByte(inputBase, input));

            input--;
            state1 = initialize(table, UNSAFE.getByte(inputBase, input));
        }

        // join to mod 4
        inputSize -= 2;

        if ((SIZE_OF_LONG * 8 > FiniteStateEntropy.MAX_TABLE_LOG * 4 + 7) && (inputSize & 2) != 0) {  /* test bit 2 */
            input--;
            state2 = encode(stream, table, state2, UNSAFE.getByte(inputBase, input));

            input--;
            state1 = encode(stream, table, state1, UNSAFE.getByte(inputBase, input));

            stream.flush();
        }

        // 2 or 4 encoding per loop
        while (input > start) {
            input--;
            state2 = encode(stream, table, state2, UNSAFE.getByte(inputBase, input));

            if (SIZE_OF_LONG * 8 < FiniteStateEntropy.MAX_TABLE_LOG * 2 + 7) {
                stream.flush();
            }

            input--;
            state1 = encode(stream, table, state1, UNSAFE.getByte(inputBase, input));

            if (SIZE_OF_LONG * 8 > FiniteStateEntropy.MAX_TABLE_LOG * 4 + 7) {
                input--;
                state2 = encode(stream, table, state2, UNSAFE.getByte(inputBase, input));

                input--;
                state1 = encode(stream, table, state1, UNSAFE.getByte(inputBase, input));
            }

            stream.flush();
        }

        flush(stream, table, state2);
        flush(stream, table, state1);

        return stream.close();
    }

    public static int initialize(FseCompressionTable fse, byte symbol)
    {
        int outputBits = (fse.deltaNumberOfBits[symbol] + (1 << 15)) >>> 16;
//        DebugLog.print("FSE initialize: symbol=%d, deltaNbBits=%d, deltaFindState=%d, nbBitsOut=%d", symbol, fse.deltaNumberOfBits[symbol], fse.deltaFindState[symbol], outputBits);
        int base = ((outputBits << 16) - fse.deltaNumberOfBits[symbol]) >>> outputBits;
        return fse.nextState[base + fse.deltaFindState[symbol]];
    }

    public static int encode(BitstreamEncoder stream, FseCompressionTable fse, int state, int symbol)
    {
        int outputBits = (state + fse.deltaNumberOfBits[symbol]) >>> 16;
        stream.addBits(state, outputBits);

//        DebugLog.print("FSE encode: symbol: %d, state: %d, bits: %d", symbol, state, outputBits);

        return fse.nextState[(state >>> outputBits) + fse.deltaFindState[symbol]];
    }

    public static void flush(BitstreamEncoder stream, FseCompressionTable fse, int state)
    {
        stream.addBits(state, fse.log2Size);
        stream.flush();
    }
}
