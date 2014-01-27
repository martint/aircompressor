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
package io.airlift.compress.lz4;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class Lz4SliceFixture
    extends Fixture
{
    private Slice output;
    private Slice compressed;

    @Setup
    public void setup()
    {
        compressed = compress(getData());
        output = Slices.allocate(getData().length());
    }

    public Slice getOutput()
    {
        return output;
    }

    public Slice getCompressed()
    {
        return compressed;
    }

    @TearDown(Level.Iteration)
    public void check()
    {
        assertEquals(output.getBytes(), getData().getBytes());
    }

    private Slice compress(Slice uncompressed)
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length());

        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(uncompressed.getBytes(), 0, uncompressed.length(), compressedBytes, 0);

        return Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, compressedLength));
    }


}
