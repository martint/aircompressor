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

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestLz4
{
    @Test
    public void testLz4()
            throws Exception
    {
        Slice compressed = getCompressedData();

        Slice decompressedActual = Slices.allocate(getUncompressedData().length());
        int written = new Lz4Decompressor().uncompress(compressed, 0, compressed.length(), decompressedActual, 0);

        if (!decompressedActual.equals(getUncompressedData())) {
            throw new AssertionError(String.format(
                    "Actual   : %s\n" +
                            "Expected : %s",
                    Arrays.toString(decompressedActual.getBytes()),
                    Arrays.toString(getUncompressedData().getBytes())));
        }


    }

    private Slice getCompressedData()
            throws IOException
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        Slice uncompressed = getUncompressedData();
        int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length());

        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(uncompressed.getBytes(), 0, uncompressed.length(), compressedBytes, 0);

        return Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, compressedLength));
    }

    private Slice getUncompressedData()
            throws IOException
    {
        return Slices.mapFileReadOnly(new File("testdata/html"));
//        return Slices.copiedBuffer("abababababababababacababababa", StandardCharsets.UTF_8);
    }
}
