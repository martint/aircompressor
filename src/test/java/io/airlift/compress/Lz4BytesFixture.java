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

import io.airlift.compress.Fixture;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class Lz4BytesFixture
    extends Fixture
{
    private byte[] output;
    private byte[] compressed;

    @Setup
    public void setup()
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(getUncompressed().length);

        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(getUncompressed(), 0, getUncompressed().length, compressedBytes, 0);

        compressed = Arrays.copyOf(compressedBytes, compressedLength);
        output = new byte[getUncompressed().length];
    }

    public byte[] getOutput()
    {
        return output;
    }

    public byte[] getCompressed()
    {
        return compressed;
    }

    @TearDown(Level.Iteration)
    public void check()
    {
        assertEquals(output, getUncompressed());
    }
}