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

import static io.airlift.compress.zstd.HuffmanCompressionContext.MAX_SYMBOL_COUNT;

public class HuffmanCompressionTable
{
    // HUF_CElt
    short[] values = new short[MAX_SYMBOL_COUNT];
    byte[] numberOfBits = new byte[MAX_SYMBOL_COUNT];

    public void trim(int maxSymbolValue)
    {
        Arrays.fill(values, maxSymbolValue + 1, values.length, (short) 0);
        Arrays.fill(numberOfBits, maxSymbolValue + 1, values.length, (byte) 0);
    }

    public void copyFrom(HuffmanCompressionTable other)
    {
        System.arraycopy(other.values, 0, values, 0, values.length);
        System.arraycopy(other.numberOfBits, 0, numberOfBits, 0, numberOfBits.length);
    }
}
