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
package io.airlift.compress;

import io.airlift.compress.lz4.Lz4CompressorNew;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.File;
import java.io.IOException;

public class TestCompressor
{
    public static void main(String[] args)
            throws IOException
    {
        Slice raw = Slices.mapFileReadOnly(new File("testdata/html"));
        Slice uncompressed = Slices.copyOf(raw);

        LZ4Compressor jpountz = LZ4Factory.safeInstance().fastCompressor();
        Lz4CompressorNew airlift = new Lz4CompressorNew();

        int maxCompressedLength = jpountz.maxCompressedLength(uncompressed.length());

        Slice actual = Slices.allocate(maxCompressedLength);
        int actualSize = airlift.compress(uncompressed, 0, uncompressed.length(), actual, 0);

        byte[] expected = new byte[maxCompressedLength];
        int expectedSize = jpountz.compress(uncompressed.getBytes(), 0, uncompressed.length(), expected, 0);

        System.out.println("actual size:   " + actualSize);
        System.out.println("expected size: " + expectedSize);

//        for (int i = 0; i < Math.min(actualSize, expectedSize); i++) {
//            String same = "";
//            if (actual.getByte(i) != expected[i]) {
//                same = "<<<<";
//            }
//            System.out.printf("%5d: %02x %02x %s\n", i, actual.getByte(i), expected[i], same);
//        }

        Slice x = Slices.allocate(uncompressed.length());
        new Lz4Decompressor().uncompress(actual, 0, actualSize, x, 0);


    }
}
