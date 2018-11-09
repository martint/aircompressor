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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class X
{
    public static void main(String[] args)
            throws Exception
    {
        ZstdCompressor compressor = new ZstdCompressor();

//        Path path = Paths.get("testdata", "silesia", "mr");
//        Path path = Paths.get("/usr/local/fb-flake8/flake8-2/future/types/newmemoryview.pyc");
//        byte[] original = Files.readAllBytes(path);

        byte[] original = new byte[10000];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) ThreadLocalRandom.current().nextInt(128);
        }

        Files.write(Paths.get("uniform_ascii.bin"), original);
        
        // Files.readAllBytes(Paths.get("testdata", "silesia", "xml"));

        int maxCompressLength = compressor.maxCompressedLength(original.length);
        byte[] compressed = new byte[maxCompressLength];
        byte[] control = new byte[maxCompressLength];
        byte[] decompressed = new byte[original.length];

        int controlSize = new ZstdJniCompressor(3).compress(original, 0, original.length, control, 0, control.length);
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        System.out.printf("compressed size = " + compressedSize);
//        System.out.println(controlSize + " vs " + compressedSize);
//        int compressedSize = 0;
//        for (int i = 0; i < 1000; i++) {
//            compressor.compress(original, 0, original.length, compressed, 0, compressed.length);
//        }


        System.err.println("decompressing");
//        for (int i = 0; i < compressedSize; i++) {
//            System.out.println("main.c: " + compressed[i]);
//        }

//        AbstractTestCompression.assertByteArraysEqual(compressed, 0, compressedSize, control, 0, controlSize - 4); // don't include checksum

        Files.write(Paths.get("corrupted.zst"), Arrays.copyOf(compressed, compressedSize));
        Files.write(Paths.get("control.zst"), Arrays.copyOf(control, controlSize));

        int decompressedSize = new ZstdDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);
//        int decompressedSize = new ZstdJniDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);
//        AbstractTestCompression.assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);

        int diffs = 0;
        for (int i = 0; i < Math.min(compressedSize, controlSize) && diffs < 10; i++) {
            if (compressed[i] != control[i]) {
                System.out.printf("Difference at offset %d, ours = 0x%x, theirs=0x%x\n", i, compressed[i], control[i]);
                diffs++;
            }
        }
    }

}
