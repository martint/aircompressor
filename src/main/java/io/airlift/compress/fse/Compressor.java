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
package io.airlift.compress.fse;

import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.fse.UnsafeUtil.UNSAFE;
import static io.airlift.compress.fse.Util.verify;

public class Compressor
{
    public static int compress(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize, CompressionTable table)
    {
        verify(outputSize >= SIZE_OF_LONG, -1, "Output size too small"); // TODO

        long start = inputAddress;
        long inputLimit = start + inputSize;

        long input = inputLimit;

        if (inputSize <= 2) {
            return 0;
        }

        BitstreamEncoder stream = new BitstreamEncoder(outputBase, outputAddress, outputSize);

        int state;

        input--;
        state = initialize(table, UNSAFE.getByte(inputBase, input));

        // 2 or 4 encoding per loop
        while (input > start) {
            input--;
            state = encode(stream, table, state, UNSAFE.getByte(inputBase, input));

            // TODO: when to flush?
             stream.flush();
        }

        flush(stream, table, state);

        return stream.close();
    }

    private static int initialize(CompressionTable fse, byte symbol)
    {
        int outputBits = (fse.deltaNumberOfBits[symbol] + (1 << 15)) >>> 16;
        int base = ((outputBits << 16) - fse.deltaNumberOfBits[symbol]) >>> outputBits;
        return fse.nextState[base + fse.deltaFindState[symbol]];
    }

    private static int encode(BitstreamEncoder stream, CompressionTable fse, int state, int symbol)
    {
        int outputBits = (state + fse.deltaNumberOfBits[symbol]) >> 16;
        stream.addBits(state, outputBits);
        return fse.nextState[(state >>> outputBits) + fse.deltaFindState[symbol]];
    }

    private static void flush(BitstreamEncoder stream, CompressionTable fse, int state)
    {
        stream.addBits(state, fse.log2Size);
        stream.flush();
    }
}

