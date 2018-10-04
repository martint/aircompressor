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

import io.airlift.compress.thirdparty.ZstdJniCompressor;

public class X
{
    public static void main(String[] args)
            throws Exception
    {
        byte[] data = new byte[0]; //Files.readAllBytes(Paths.get("testdata", "silesia", "xml"));
        byte[] compressed = new byte[data.length * 2 + 100];
        byte[] compressed2 = new byte[data.length * 2 + 100];
        byte[] decompressed = new byte[data.length];

        int compressedSize = new ZstdJniCompressor(0).compress(data, 0, data.length, compressed, 0, compressed.length);

        int compressedSize2 = ZstdFrameCompressor.compress(data, 16, 16 + data.length, compressed2, 16, 16 + compressed2.length, 3);

        int decompressedSize = new ZstdDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        System.out.println("Original size:     " + data.length);
        System.out.println("Compressed size:   " + compressedSize);
        System.out.println("Decompressed size: " + decompressedSize);
    }

}
