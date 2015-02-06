///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.airlift.compress.lz4;
//
//import io.airlift.slice.Slice;
//
//import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;
//
//public class Lz4Compressor
//{
//    private final static ControlException EXCEPTION = new ControlException();
//
//    private final static int COMPRESSION_LEVEL = 12;
//    private final static int NOT_COMPRESSIBLE_CONFIRMATION = 5;
//
//    private final static int HASH_BITS = COMPRESSION_LEVEL + 1;
//    private final static int TABLESIZE = 1 << HASH_BITS;
//
//    private final static int COPY_LENGTH = 8;
//
//    private final static int MIN_MATCH = 4;
//    private final static int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;
//
//    private static final int HASH_SHIFT = ((MIN_MATCH * 8) - HASH_BITS);
//
//    private final static int LAST_LITERALS = 5;
//    private final static int SKIP_STRENGTH = Math.max(2, NOT_COMPRESSIBLE_CONFIRMATION);
//
//    private final static int ML_BITS = 4;
//    private final static int ML_MASK = (1 << ML_BITS) - 1;
//    private final static int RUN_BITS = 8 - ML_BITS;
//    private final static int RUN_MASK = (1 << RUN_BITS) - 1;
//
//    private final static int MIN_LENGTH = MATCH_FIND_LIMIT + 1;
//
//    private final static int BLOCK_SIZE = 1 << 16;
//
//    public static int maxCompressedLength(int sourceLength)
//    {
//        return (sourceLength + (sourceLength / 255) + 16);
//    }
//
//    public int compress(Slice uncompressed, int uncompressedOffset, int uncompressedSize, Slice compressed, int compressedOffset)
//    {
//
//    }
//
//    private static long compress(
//            final Object inputBase,
//            final long inputAddress,
//            final int inputLength,
//            final Object outputBase,
//            final long outputAddress,
//            final short[] table)
//    {
//        long input = inputAddress;
//        long output = outputAddress;
//
//        final long inputLimit = inputAddress + inputLength;
//        final long matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
//        final long matchLimit = inputLimit - LAST_LITERALS;
//
//        if (inputLength < MIN_LENGTH) {
//            output = emitLastLiteral(outputBase, output, inputBase, input, inputLimit - input);
//            return output - outputAddress;
//        }
//
//        long anchor = input;
//        input++;
//        int nextHash = hash(UNSAFE.getInt(inputBase, input));
//
//        boolean done = false;
//        do {
//            int findMatchAttempts = (1 << SKIP_STRENGTH) + 3;
//
//            long nextInputIndex = input;
//            long referenceIndex;
//
//            // find 4-byte match
//            do {
//                int hash = nextHash;
//                input = nextInputIndex;
//
//                int step = findMatchAttempts++ >> SKIP_STRENGTH;
//                nextInputIndex = input + step;
//
//                if (nextInputIndex > matchFindLimit) {
//                    return emitLastLiteral(outputBase, output, inputBase, anchor, inputLimit - anchor);
//                }
//
//                nextHash = hash(UNSAFE.getInt(inputBase, nextInputIndex));
//                referenceIndex = input + (table[hash] & 0xFFFF);
//                table[hash] = (short) (input - inputOffset);
//            }
//            while (loadInt(input, referenceIndex) != loadInt(input, input));
//
//            // catch up
//            while (input > anchor && (referenceIndex > inputOffset) && input[input - 1] == input[referenceIndex - 1]) {
//                --input;
//                --referenceIndex;
//            }
//
//            int length = input - anchor;
//            int tokenAddress = output++;
//
//            output = encodeRunLength(output, tokenAddress, output, length);
//
//            // emitLiteral copies in chunks of 8 bytes. We are guaranteed to have at least 8 bytes available in output (how?)
//            output = emitLiteral(output, output, input, anchor, length);
//
//            while (true) {
//                output = encodeOffset(output, output, input - referenceIndex);
//
//                // find match length
//                input += MIN_MATCH;
//                referenceIndex += MIN_MATCH;
//                anchor = input;
//
//                // first, compare long at a time
//                try {
//                    while (input < matchLimit - 7) {
//                        long diff = loadLong(input, referenceIndex) ^ loadLong(input, input);
//
//                        if (diff != 0) {
//                            input += Long.numberOfTrailingZeros(diff) >> 3;
//                            throw EXCEPTION;
//                        }
//
//                        input += 8;
//                        referenceIndex += 8;
//                    }
//
//                    if (input < matchLimit - 3 && loadInt(input, referenceIndex) == loadInt(input, input)) {
//                        input += 4;
//                        referenceIndex += 4;
//                    }
//
//                    if (input < matchLimit - 1 && loadShort(input, referenceIndex) == loadShort(input, input)) {
//                        input += 2;
//                        referenceIndex += 2;
//                    }
//
//                    if (input < matchLimit && input[referenceIndex] == input[input]) {
//                        ++input;
//                    }
//                }
//                catch (ControlException e) {
//                }
//
//                // Encode MatchLength
//                output = encodeMatchLength(outputBase, tokenAddress, output, input - anchor);
//
//                // are we done?
//                if (input > matchFindLimit) {
//                    done = true;
//                    anchor = input;
//                    break;
//                }
//
//                // update hash table to improve match probability
//                int z = input - 2;
//                table[hash(loadInt(input, z))] = (short) (z - inputOffset);
//
//                // Test next position
//                int x = loadInt(input, input);
//                int y = hash(x);
//                referenceIndex = inputOffset + (table[y] & 0xFFFF);
//                table[y] = (short) (input - inputOffset);
//
//                if (loadInt(input, referenceIndex) != x) {
//                    // Prepare next major loop
//                    anchor = input++;
//                    nextHash = hash(loadInt(input, input));
//                    break;
//                }
//
//                // go for another match
//                tokenAddress = output++;
//                output[tokenAddress] = 0;
//            }
//        }
//        while (!done);
//
//        // Encode Last Literals
//        output = emitLastLiteral(output, output, input, anchor, inputLimit - anchor);
//
//        return output;
//    }
//
//    private static int hash(int value)
//    {
//        return (int) ((value * 2654435761L) & 0xFFFFFFFFL) >>> HASH_SHIFT;
//    }
//
//    private static long emitLiteral(
//            final Object outputBase,
//            final long outputAddress,
//            final Object inputBase,
//            final long inputAddress,
//            final long length)
//    {
//        final long outputLimit = outputAddress + length;
//
//        long output = outputAddress;
//        long input = inputAddress;
//        do {
//            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
//            input += 8;
//            output += 8;
//        }
//        while (output < outputLimit);
//
//        return outputLimit;
//    }
//
//    private static long emitLastLiteral(
//            final Object outputBase,
//            final long outputAddress,
//            final Object inputBase,
//            final long inputAddress,
//            final long length)
//    {
//        long output = encodeRunLength(outputBase, outputAddress, outputAddress + 1, length);
//        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress, length);
//
//        return output + length;
//    }
//
//    private static long encodeOffset(final Object outputBase, final long outputAddress, final int offset)
//    {
//        UNSAFE.putShort(outputBase, outputAddress, (short) offset);
//        return outputAddress + 2;
//    }
//
//    private static long encodeRunLength(
//            final Object base,
//            final long tokenAddress,
//            final long continuationAddress,
//            final long length)
//    {
//        long output = continuationAddress;
//        if (length >= RUN_MASK) {
//            UNSAFE.putByte(base, tokenAddress, (byte) (RUN_MASK << ML_BITS));
//            long remaining = length - RUN_MASK;
//            while (remaining > 254) {
//                UNSAFE.putByte(base, output++, (byte) 255);
//                remaining -= 255;
//            }
//            UNSAFE.putByte(base, output++, (byte) remaining);
//        }
//        else {
//            UNSAFE.putByte(base, tokenAddress, (byte) (length << ML_BITS));
//        }
//
//        return output;
//    }
//
//    private static long encodeMatchLength(
//            final Object outputBase,
//            final long tokenAddress,
//            final long continuationAddress,
//            final long length)
//    {
//        long output = continuationAddress;
//        if (length >= ML_MASK) {
//            long remaining = length;
//            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) + ML_MASK));
//            remaining -= ML_MASK;
//            while (remaining > 509) {
//                // todo: putshort?
//                UNSAFE.putByte(outputBase, output++, (byte) 255);
//                UNSAFE.putByte(outputBase, output++, (byte) 255);
//                remaining -= 510;
//            }
//            if (remaining > 254) {
//                UNSAFE.putByte(outputBase, output++, (byte) 255);
//                remaining -= 255;
//            }
//            UNSAFE.putByte(outputBase, output++, (byte) remaining);
//        }
//        else {
//            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) + length));
//        }
//
//        return continuationAddress;
//    }
//
//    private final static class ControlException extends Exception {}
//}
