/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package io.airlift.compress;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

class UnsafeMemory
        implements Memory
{
    private static final Unsafe unsafe;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
            // It seems not all Unsafe implementations implement the following method.
            new UnsafeMemory().copyMemory(new byte[1], 0, new byte[1], 0, 1);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final long BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    private static final long SHORT_ARRAY_OFFSET = unsafe.arrayBaseOffset(short[].class);
    private static final long SHORT_ARRAY_STRIDE = unsafe.arrayIndexScale(short[].class);

    @Override
    public boolean fastAccessSupported()
    {
        return true;
    }


    public static long inputAddress;
    public static long inputLength;
    public static long outputAddress;
    public static long outputLength;

    public static void checkAddress(long address, long length)
    {
//        if (address >= inputAddress && address + length <= inputAddress + inputLength) {
//            return;
//        }
//        if (address >= outputAddress && address + length <= outputAddress + outputLength) {
//            return;
//        }
//        throw new IllegalStateException(String.format("address=%d length=%d inputAddress=%d inputLength=%d outputAddress=%d outputLength=%d",
//                address,
//                length,
//                inputAddress,
//                inputLength,
//                outputAddress,
//                outputLength));
    }

    public static int readByte(long address)
    {
        checkAddress(address, 1);
        return unsafe.getByte(address) & 0xFF;
    }

    public static int readShort(long address)
    {
        checkAddress(address, 2);
        return unsafe.getShort(address);
    }

    public static int readInt(long address)
    {
        checkAddress(address, 4);
        return unsafe.getInt(address);
    }

    public static void copyByte(long outputAddress, long inputAddress)
    {
        checkAddress(outputAddress, 1);
        checkAddress(inputAddress, 1);
        unsafe.putByte(outputAddress, unsafe.getByte(inputAddress));
    }

    public static void copyInt(long outputAddress, long inputAddress)
    {
        checkAddress(outputAddress, 4);
        checkAddress(inputAddress, 4);
        unsafe.putInt(outputAddress, unsafe.getInt(inputAddress));
    }


    public static void copyLong(long outputAddress, long inputAddress)
    {
        checkAddress(outputAddress, 8);
        checkAddress(inputAddress, 8);

        unsafe.putLong(outputAddress, unsafe.getLong(inputAddress));
    }

    public static void copyMemory(long inputAddress, long outputAddress, int length)
    {
        unsafe.copyMemory(inputAddress, outputAddress, length);
    }

    @Override
    public int lookupShort(short[] data, int index)
    {
        assert index >= 0;
        assert index <= data.length;
        return unsafe.getShort(data, SHORT_ARRAY_OFFSET + (index * SHORT_ARRAY_STRIDE)) & 0xFFFF;
    }

    @Override
    public int loadByte(byte[] data, int index)
    {
        assert index >= 0;
        assert index <= data.length;
        return unsafe.getByte(data, BYTE_ARRAY_OFFSET + index) & 0xFF;
    }

    @Override
    public int loadShort(byte[] data, int index)
    {
        assert index >= 0;
        assert index + 2 <= data.length;
        return unsafe.getShort(data, BYTE_ARRAY_OFFSET + index) & 0xFFFF;
    }

    @Override
    public void writeShort(byte[] data, int index, short value)
    {
        assert index >= 0;
        assert index + 2 <= data.length;
        unsafe.putShort(data, BYTE_ARRAY_OFFSET + index, value);
    }

    @Override
    public int loadInt(byte[] data, int index)
    {
        assert index >= 0;
        assert index + 4 <= data.length;
        return unsafe.getInt(data, BYTE_ARRAY_OFFSET + index);
    }

    @Override
    public void writeInt(byte[] data, int index, int value)
    {
        assert index >= 0;
        assert index + 4 <= data.length;
        unsafe.putInt(data, BYTE_ARRAY_OFFSET + index, value);
    }

    @Override
    public void writeLong(byte[] data, int index, long value)
    {
        assert index >= 0;
        assert index + 8 <= data.length;
        unsafe.putLong(data, BYTE_ARRAY_OFFSET + index, value);
    }

    @Override
    public void copyInt(byte[] src, int srcIndex, byte[] dest, int destIndex)
    {
        assert srcIndex >= 0;
        assert srcIndex + 4 <= src.length;
        assert destIndex >= 0;
        assert destIndex + 4 <= dest.length;
        int value = unsafe.getInt(src, BYTE_ARRAY_OFFSET + srcIndex);
        unsafe.putInt(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
    }


    @Override
    public void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
    {
        assert srcIndex >= 0;
        assert srcIndex + 8 <= src.length;
        assert destIndex >= 0;
        assert destIndex + 8 <= dest.length;
        long value = unsafe.getLong(src, BYTE_ARRAY_OFFSET + srcIndex);
        unsafe.putLong(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
    }

    @Override
    public long loadLong(byte[] data, int index)
    {
        assert index > 0;
        assert index + 4 < data.length;
        return unsafe.getLong(data, BYTE_ARRAY_OFFSET + index);
    }

    @Override
    public void copyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int length)
    {
        assert inputIndex >= 0;
        assert inputIndex + length <= input.length;
        assert outputIndex >= 0;
        assert outputIndex + length <= output.length;
        unsafe.copyMemory(input, UnsafeMemory.BYTE_ARRAY_OFFSET + inputIndex, output, UnsafeMemory.BYTE_ARRAY_OFFSET + outputIndex, length);
    }
}
