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

import static io.airlift.compress.fse.UnsafeUtil.UNSAFE;

public class BitstreamEncoder
{
    private static final int[] BIT_MASK = {
            0, 1, 3, 7, 0xF, 0x1F,
            0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF,
            0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF, 0x1FFFF,
            0x3FFFF, 0x7FFFF, 0xFFFFF, 0x1FFFFF, 0x3FFFFF, 0x7FFFFF,
            0xFFFFFF, 0x1FFFFFF, 0x3FFFFFF, 0x7FFFFFF, 0xFFFFFFF, 0x1FFFFFFF,
            0x3FFFFFFF, 0x7FFFFFFF}; /* up to 31 bits */

    private final Object outputBase;
    private final long outputAddress;
    private final long outputLimit;

    private long container;
    private int bitCount;
    private long currentAddress;

    public BitstreamEncoder(Object outputBase, long outputAddress, int outputSize)
    {
        this.outputBase = outputBase;

        container = 0;
        bitCount = 0;
        this.outputAddress = outputAddress;
        outputLimit = this.outputAddress + outputSize;

        currentAddress = this.outputAddress;
    }

    public void addBits(int value, int bitCount)
    {
        container |= (value & BIT_MASK[bitCount]) << this.bitCount;
        this.bitCount += bitCount;
    }

    public void flush()
    {
        int bytes = bitCount >>> 3;
        UNSAFE.putLong(outputBase, currentAddress, container);
        currentAddress += bytes;

        if (currentAddress > outputLimit) {
            currentAddress = outputLimit;
        }

        bitCount &= 7;
        container >>>= bytes * 8;
    }

    public int close()
    {
        addBits(1, 1); // TODO addBitsFast  /* endMark */
        flush();

        if (currentAddress >= outputLimit) {
            return 0; /* overflow detected */
        }
        
        return (int) ((currentAddress - outputAddress) + (bitCount > 0 ? 1 : 0));
    }
}
